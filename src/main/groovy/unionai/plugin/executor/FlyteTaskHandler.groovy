package unionai.plugin.executor

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException

import flyteidl2.actions.ActionsServiceOuterClass.Action
import flyteidl2.common.Phase.ActionPhase
import flyteidl2.task.Common.Outputs
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.exception.ProcessException
import nextflow.exception.ProcessUnrecoverableException
import nextflow.extension.FilesEx
import nextflow.file.FileHelper
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import nextflow.trace.TraceRecord
import unionai.flyte.controller.ActionState
import unionai.flyte.controller.Submitted
import unionai.plugin.client.InputsProto
import unionai.plugin.client.InputsProto.BlobInput
import unionai.plugin.executor.FlyteTaskLauncher.StagedInput

/**
 * Runs one Nextflow task as a Flyte child action. Its state comes from the executor's
 * {@link unionai.flyte.controller.ActionController}, which watches it: checking it never calls Flyte.
 */
@Slf4j
@CompileStatic
class FlyteTaskHandler extends TaskHandler {

    /** Where copilot's `outputs.pb` is, under the action's output URI */
    static final String OUTPUTS_FILE = 'outputs.pb'
    /**
     * The executor reads a task's inputs from `<input_uri prefix>/inputs.pb`: it strips a trailing
     * `/inputs.pb` and appends it again, so the file must have exactly that name.
     */
    static final String INPUTS_FILE = '.flyte/inputs.pb'
    static final String INPUTS_NAME = 'inputs.pb'


    private final FlyteExecutor executor
    private final Path outputFile
    private final Path errorFile
    private final Path exitFile

    @PackageScope String actionName
    /** The task's {@link #cacheKey()}, computed once on submit */
    @PackageScope String taskKey
    @PackageScope List<StagedInput> stagedInputs
    /** The uploaded bundle of the task's script, for the action's Code tab (see {@link CodeBundles}) */
    @PackageScope volatile String codeBundleUri
    /** The enqueue in flight, until {@link #enqueued()} has seen it complete */
    @PackageScope CompletableFuture<Submitted> submission
    private Map<String,BlobInput> inputs
    private boolean enqueued
    /**
     * Guards {@link #actionName} and {@link #submission} where the poll thread replaces them (a
     * resubmit) while another thread kills the task, and records the kill, after which nothing is
     * submitted any more.
     */
    private final Object submitLock = new Object()
    private boolean killed

    FlyteTaskHandler(TaskRun task, FlyteExecutor executor) {
        super(task)
        this.executor = executor
        this.outputFile = task.workDir.resolve(TaskRun.CMD_OUTFILE)
        this.errorFile = task.workDir.resolve(TaskRun.CMD_ERRFILE)
        this.exitFile = task.workDir.resolve(TaskRun.CMD_EXIT)
    }

    @Override
    void prepareLauncher() {
        stagedInputs = planInputs(task.getInputFilesMap())
        FlyteTaskLauncher.create(task, stagedInputs).build()
    }

    /**
     * Give every staged input its own copilot input (`in_0`, `in_1`, ...), as a Dir when it is a
     * directory. The files already live in the work dir or another object store location:
     * Nextflow copies foreign inputs (local, http, ...) there before the task is submitted.
     */
    static List<StagedInput> planInputs(Map<String,Path> inputFiles) {
        final result = new ArrayList<StagedInput>(inputFiles.size())
        int i = 0
        for( Map.Entry<String,Path> it : inputFiles.entrySet() )
            result.add(new StagedInput("in_${i++}".toString(), it.value, it.key, Files.isDirectory(it.value)))
        return result
    }

    @Override
    void submit() {
        if( !task.container )
            throw new ProcessUnrecoverableException("Process `${task.lazyName()}` failed because the `flyte` executor requires a container image -- set the `container` directive")

        taskKey = cacheKey()
        actionName = actionNameFor(task, taskKey)
        inputs = writeInputs()
        inputsUri = storeInputs(actionName)
        synchronized( submitLock ) {
            if( killed )
                return
            // the script's upload and then the enqueue complete in the background: the monitor
            // moves on to the next task. A kill in between aborts the action once it's enqueued.
            submission = executor.codeBundles.upload(task.workDir.resolve(TaskRun.CMD_SCRIPT)).thenCompose(this.&enqueue)
        }
        status = TaskStatus.SUBMITTED
        log.debug "[FLYTE] Process `${task.lazyName()}` submitted > action=$actionName; work-dir=${task.workDirStr}"
    }

    /**
     * Enqueue the task's action, with the uploaded bundle of its script (null: no code view)
     */
    protected CompletableFuture<Submitted> enqueue(String codeBundleUri) {
        this.codeBundleUri = codeBundleUri
        return executor.controller.submit(newAction(inputs))
    }

    /**
     * @return whether the task's action is enqueued; false while the enqueue is in flight
     * @throws ProcessException when the enqueue failed
     */
    @PackageScope
    boolean enqueued() {
        if( enqueued )
            return true
        if( !submission.isDone() )
            return false
        final result = submitted()
        if( result == Submitted.EXISTS && phase() == ActionPhase.ACTION_PHASE_ABORTED && resubmit() )
            return false
        if( result == Submitted.EXISTS )
            log.debug "[FLYTE] Action $actionName already exists, attaching to it"
        enqueued = true
        return true
    }

    /**
     * The failed head aborted this action when it shut down (Nextflow kills its running tasks on
     * SIGTERM): run the task again, under its hash, which is new in this session.
     *
     * @return whether the task was submitted again
     */
    private boolean resubmit() {
        final hashName = FlyteActionBuilder.actionName(task.hash.toString())
        if( actionName == hashName )
            return false
        log.debug "[FLYTE] Action $actionName was aborted by a previous attempt, running the task again"
        final uri = storeInputs(hashName)
        synchronized( submitLock ) {
            if( killed )
                return false
            actionName = hashName
            inputsUri = uri
            submission = executor.controller.submit(newAction(inputs))
        }
        return true
    }

    private Submitted submitted() {
        try {
            return submission.get()
        }
        catch( ExecutionException e ) {
            throw new ProcessException("Unable to submit Flyte action $actionName: ${e.cause?.message}", e.cause)
        }
    }

    private ActionState state() {
        return executor.controller.state(actionName)
    }

    private ActionPhase phase() {
        return state()?.phase
    }

    /**
     * Name a cacheable task's action after its cache key and attempt, not its Nextflow hash: when
     * a retried head resumes a task that was still running, Nextflow finds its work dir and moves
     * it to a new hash, but the key and attempt stay the same, so the enqueue attaches to the
     * running action instead of starting another. A process with `cache false` keeps its hash,
     * so two runs of it with the same inputs stay separate actions.
     */
    static String actionNameFor(TaskRun task, String key) {
        return task.processor.isCacheable()
            ? FlyteActionBuilder.actionName(key, task.failCount)
            : FlyteActionBuilder.actionName(task.hash.toString())
    }

    /**
     * Encode the action's `flyteidl2.task.Inputs`; {@link #storeInputs} writes them.
     *
     * @return the inputs' locations, by name
     */
    protected Map<String,BlobInput> writeInputs() {
        final inputs = new LinkedHashMap<String,BlobInput>()
        for( StagedInput it : stagedInputs )
            inputs.put(it.var, new BlobInput(it.var, FilesEx.toUriString(it.source), it.dir))
        // the rendered wrapper and task script, plus the optional stage script and stdin
        for( String name : [TaskRun.CMD_RUN, TaskRun.CMD_SCRIPT, TaskRun.CMD_STAGE, TaskRun.CMD_INFILE] ) {
            final path = task.workDir.resolve(name)
            if( name in [TaskRun.CMD_RUN, TaskRun.CMD_SCRIPT] || Files.exists(path) ) {
                final var = 'nf_cmd_' + name.replace('.command.', '')
                inputs.put(var, new BlobInput(var, FilesEx.toUriString(path), false))
            }
        }
        if( executor.remoteBinDir ) {
            final bin = FlyteTaskLauncher.BIN_INPUT
            inputs.put(bin, new BlobInput(bin, FilesEx.toUriString(executor.remoteBinDir), true))
        }
        inputsBytes = InputsProto.encode(new ArrayList<BlobInput>(inputs.values()))
        return inputs
    }

    private byte[] inputsBytes
    @PackageScope String inputsUri

    /**
     * Store the encoded inputs where Flyte keeps the named action's data, under the run's
     * storage, as the Flyte SDK does: Flyte's UI only shows inputs from there. The task hash in
     * the path keeps a resumed head from overwriting the inputs of the action it attaches to.
     *
     * When the run's storage isn't writable (e.g. out of reach of the credentials Nextflow runs
     * with), the inputs go to the task's work dir, for this and every later task: they still
     * run, the UI just doesn't show their inputs.
     *
     * @return the inputs' URI
     */
    protected String storeInputs(String name) {
        if( !executor.inputsInWorkDir ) {
            final base = executor.flyteConfig.runOutputBase.replaceFirst(/\/+$/, '')
            try {
                final target = FileHelper.asPath("$base/$name/${task.hash}/$INPUTS_NAME")
                Files.createDirectories(target.parent)
                Files.write(target, inputsBytes)
                return FilesEx.toUriString(target)
            }
            catch( Exception e ) {
                executor.inputsInWorkDir = true
                log.warn "Unable to write task inputs under the Flyte run's storage ($base), writing them to the work dir instead: Flyte's UI won't show task inputs -- ${e.message}"
            }
        }
        final target = task.workDir.resolve(INPUTS_FILE)
        Files.createDirectories(target.parent)
        Files.write(target, inputsBytes)
        return FilesEx.toUriString(target)
    }

    protected Action newAction(Map<String,BlobInput> inputs) {
        final builder = new FlyteActionBuilder(executor.flyteConfig)
        builder.name = actionName
        builder.shortName = FlyteActionBuilder.shortName(task.processor.name)
        builder.group = task.processor.name
        builder.image = task.container
        builder.cpus = task.config.getCpus()
        builder.memory = task.config.getMemory()
        builder.disk = task.config.getDisk()
        builder.gpus = task.config.getAccelerator()?.request
        builder.time = task.config.getTime()
        builder.inputsUri = inputsUri
        builder.codeBundleUri = codeBundleUri
        builder.inputs = inputs.collectEntries { name, input -> [name, input.dir] } as Map<String,Boolean>
        if( executor.flyteConfig.cache && task.processor.isCacheable() )
            builder.cacheKey = taskKey
        return builder.build()
    }

    /**
     * Flyte cache key, stable across Nextflow sessions (see {@link FlyteCacheKey}); saved in the
     * task dir so the tasks consuming this one's outputs can chain on it.
     */
    protected String cacheKey() {
        final identity = new ArrayList<String>()
        identity.addAll([
            FlyteActionBuilder.CACHE_VERSION,
            task.processor.name,
            task.script,
            task.container,
        ])
        identity.addAll(entries(task.environment))
        // the `env` and `eval` outputs a cached .command.env must hold: `eval` ones are named by a
        // pipeline-wide counter (`nxf_out_eval_<n>`), which another pipeline revision or Nextflow
        // version numbers differently
        identity.addAll(entries(task.outputEnvNames))
        identity.addAll(entries(task.outputEvals))
        identity.addAll([
            executor.binHash,
            executor.session.stubRun ? 'stub-run' : '',
        ])
        final key = executor.cacheKeys.compute(task.getInputFilesMap(), identity)
        executor.cacheKeys.save(task.workDir, key)
        return key
    }

    /**
     * A map as cache identity entries: its size, then each name and value, sorted by name. Each
     * entry is hashed on its own, so values containing delimiters can't run into the next one.
     */
    static List<String> entries(Map<String,String> map) {
        final sorted = new TreeMap<String,String>(map ?: Collections.<String,String>emptyMap())
        final result = [sorted.size().toString()]
        sorted.each { String name, String value -> result << name << value }
        return result
    }

    /** A list as cache identity entries: its size, then each element */
    static List<String> entries(List<String> list) {
        final result = [(list?.size() ?: 0).toString()]
        result.addAll(list ?: Collections.<String>emptyList())
        return result
    }

    @Override
    boolean checkIfRunning() {
        if( !actionName || !isSubmitted() || !enqueued() )
            return false
        // null until the watch reports the action, shortly after enqueue
        if( state()?.started ) {
            status = TaskStatus.RUNNING
            return true
        }
        return false
    }

    @Override
    boolean checkIfCompleted() {
        if( !isRunning() )
            return false
        final state = state()
        if( !state?.terminal )
            return false

        task.stdout = outputFile
        task.stderr = errorFile
        if( state.succeeded )
            completeSucceeded(state)
        else
            completeFailed(state)
        status = TaskStatus.COMPLETED
        return true
    }

    /**
     * The action's `task_dir` output holds the task's outputs and `.command.*` files: copy it
     * into the Nextflow work dir, where Nextflow collects outputs and the exit status.
     */
    private void completeSucceeded(ActionState state) {
        final taskDir = taskDirOutput(state)
        if( taskDir )
            copyTree(FileHelper.asPath(taskDir), task.workDir)
        final exitCode = readExitFile()
        if( exitCode != null ) {
            task.exitStatus = exitCode
        }
        else {
            task.exitStatus = Integer.MAX_VALUE
            task.error = new ProcessException("Flyte action $actionName succeeded but its task dir has no ${TaskRun.CMD_EXIT} file (task_dir=${taskDir})")
        }
    }

    /**
     * Read the `task_dir` output from the action's `outputs.pb`, where the watch says it is, else
     * ask the Run service for the action's outputs.
     */
    @PackageScope
    String taskDirOutput(ActionState state) {
        if( state.outputUri ) {
            try {
                final file = FileHelper.asPath(state.outputUri.replaceFirst(/\/+$/, '') + '/' + OUTPUTS_FILE)
                return InputsProto.blobUri(Outputs.parseFrom(Files.readAllBytes(file)), FlyteTaskLauncher.TASK_DIR_OUTPUT)
            }
            catch( Exception e ) {
                log.debug "[FLYTE] Cannot read ${state.outputUri}/$OUTPUTS_FILE of action $actionName, asking Flyte | ${e.message}"
            }
        }
        try {
            final data = executor.runs.getActionData(FlyteActionBuilder.actionId(executor.flyteConfig, actionName))
            return InputsProto.blobUri(data.outputs, FlyteTaskLauncher.TASK_DIR_OUTPUT)
        }
        catch( Exception e ) {
            log.debug "[FLYTE] Cannot read outputs of action $actionName | ${e.message}"
            return null
        }
    }

    static void copyTree(Path source, Path target) {
        Files.walk(source).withCloseable { stream ->
            stream.filter { Path p -> !Files.isDirectory(p) }.forEach { Path file ->
                final dest = target.resolve(source.relativize(file).toString())
                // only a local work dir needs its directories: on an object store, nf-amazon & co.
                // create them as empty `dir/` objects, which copilot then fails to download as files
                if( dest.parent && dest.fileSystem.provider().scheme == 'file' )
                    Files.createDirectories(dest.parent)
                Files.copy(file, dest, StandardCopyOption.REPLACE_EXISTING)
            }
        }
    }

    private void completeFailed(ActionState state) {
        final phase = state.phase.name()
        final message = failureMessage(state)
        final failure = TaskFailure.parse(message)
        if( failure ) {
            // the task script failed: restore what Nextflow expects to find in the work dir, and
            // let its errorStrategy handle the exit status like any other failed task
            failure.writeTo(task.workDir)
            task.exitStatus = failure.exitCode
            return
        }
        // the pod failed, e.g. OOM kill, image pull failure, abort or timeout
        task.exitStatus = exitCodeFromMessage(message) ?: Integer.MAX_VALUE
        task.error = new ProcessException("Flyte action $actionName ${phase.replace('ACTION_PHASE_', '').toLowerCase()}: $message")
    }

    private Integer readExitFile() {
        try {
            return exitFile.text.trim() as Integer
        }
        catch( Exception e ) {
            log.debug "[FLYTE] Cannot read exit status for task `${task.lazyName()}` | ${e.message}"
            return null
        }
    }

    /**
     * The error the watch reported; else, e.g. for an abort, the reason from the action's details.
     */
    private String failureMessage(ActionState state) {
        if( state.error?.message )
            return state.error.message
        try {
            final details = executor.runs.getActionDetails(FlyteActionBuilder.actionId(executor.flyteConfig, actionName))
            if( details?.hasErrorInfo() && details.errorInfo.message )
                return details.errorInfo.message
            if( details?.hasAbortInfo() && details.abortInfo.reason )
                return "aborted: ${details.abortInfo.reason}"
        }
        catch( Exception e ) {
            log.debug "[FLYTE] Cannot fetch details for action $actionName | ${e.message}"
        }
        return state.phase.name()
    }

    /**
     * Flyte reports pod failures like `... terminated with exit code (137). Reason [OOMKilled] ...`;
     * surface that code so exit-status based `errorStrategy` rules (e.g. retry on 130..145) work.
     */
    static Integer exitCodeFromMessage(String message) {
        final m = message =~ /exit code \((\d+)\)/
        return m.find() ? m.group(1) as Integer : null
    }

    @Override
    protected void killTask() {
        String name
        CompletableFuture<Submitted> pending
        synchronized( submitLock ) {
            killed = true
            name = actionName
            pending = submission
        }
        if( !name || !pending )
            return
        // an enqueue still in flight would create the action after the abort: abort once it's done
        pending.handle { Submitted result, Throwable error -> abort(name); return null }
    }

    private void abort(String name) {
        // completes in the background; the executor waits briefly for pending aborts on shutdown
        executor.controller.abort(name, 'Killed by Nextflow').exceptionally { Throwable e ->
            log.debug "[FLYTE] Unable to abort action $name | ${e.message}"
            return null
        }
    }

    @Override
    TraceRecord getTraceRecord() {
        final result = super.getTraceRecord()
        if( actionName )
            result.put('native_id', actionName)
        return result
    }

    /**
     * A task script failure, as reported by the pod entry point through copilot's error file:
     * <pre>
     * nf-flyte task failed: exit=3
     * --- stderr (tail)
     * ...
     * --- stdout (tail)
     * ...
     * </pre>
     * Flyte may prefix the message (e.g. `User Error: `).
     */
    @CompileStatic
    static class TaskFailure {
        private static final String STDERR = '--- stderr (tail)'
        private static final String STDOUT = '--- stdout (tail)'

        final int exitCode
        final String stderr
        final String stdout

        TaskFailure(int exitCode, String stderr, String stdout) {
            this.exitCode = exitCode
            this.stderr = stderr
            this.stdout = stdout
        }

        static TaskFailure parse(String message) {
            if( !message )
                return null
            final m = message =~ /${java.util.regex.Pattern.quote(FlyteTaskLauncher.FAILURE_PREFIX)}(\d+)/
            if( !m.find() )
                return null
            final rest = message.substring(m.end())
            return new TaskFailure(m.group(1) as int, section(rest, STDERR, STDOUT), section(rest, STDOUT, null))
        }

        private static String section(String text, String start, String end) {
            final i = text.indexOf(start)
            if( i < 0 )
                return ''
            final from = i + start.length() + 1
            if( from > text.length() )
                return ''
            final j = end ? text.indexOf(end, from) : -1
            return (j >= 0 ? text.substring(from, j) : text.substring(from))
        }

        void writeTo(Path workDir) {
            Files.write(workDir.resolve(TaskRun.CMD_EXIT), "$exitCode\n".toString().bytes)
            Files.write(workDir.resolve(TaskRun.CMD_ERRFILE), stderr.bytes)
            Files.write(workDir.resolve(TaskRun.CMD_OUTFILE), stdout.bytes)
        }
    }
}
