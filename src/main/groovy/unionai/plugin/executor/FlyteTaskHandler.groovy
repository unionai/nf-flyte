package unionai.plugin.executor

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

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
import unionai.plugin.client.FlyteClient
import unionai.plugin.client.InputsProto
import unionai.plugin.client.InputsProto.BlobInput
import unionai.plugin.client.NotFoundException
import unionai.plugin.executor.FlyteTaskLauncher.StagedInput

/**
 * Runs one Nextflow task as a Flyte child action.
 */
@Slf4j
@CompileStatic
class FlyteTaskHandler extends TaskHandler {

    private static final Set<String> STARTED_PHASES = (['ACTION_PHASE_RUNNING'] + FlyteClient.TERMINAL_PHASES) as Set
    /**
     * The executor reads a task's inputs from `<input_uri prefix>/inputs.pb`: it strips a trailing
     * `/inputs.pb` and appends it again, so the file must have exactly that name.
     */
    static final String INPUTS_FILE = '.flyte/inputs.pb'

    private final FlyteExecutor executor
    private final Path outputFile
    private final Path errorFile
    private final Path exitFile

    @PackageScope String actionName
    @PackageScope List<StagedInput> stagedInputs

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

        actionName = FlyteActionBuilder.actionName(task.hash.toString())
        executor.client.enqueue(newAction(writeInputs()))
        status = TaskStatus.SUBMITTED
        log.debug "[FLYTE] Process `${task.lazyName()}` submitted > action=$actionName; work-dir=${task.workDirStr}"
    }

    /**
     * Write the action's `flyteidl2.task.Inputs` next to the task's other `.command.*` files.
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
        final target = task.workDir.resolve(INPUTS_FILE)
        Files.createDirectories(target.parent)
        Files.write(target, InputsProto.encode(new ArrayList<BlobInput>(inputs.values())))
        inputsUriCache = FilesEx.toUriString(target)
        return inputs
    }

    private String inputsUriCache

    protected Map newAction(Map<String,BlobInput> inputs) {
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
        builder.inputsUri = inputsUriCache
        builder.inputs = inputs.collectEntries { name, input -> [name, input.dir] } as Map<String,Boolean>
        if( executor.flyteConfig.cache && task.processor.isCacheable() )
            builder.cacheKey = cacheKey()
        return builder.build()
    }

    /**
     * Flyte cache key, stable across Nextflow sessions (see {@link FlyteCacheKey}); saved in the
     * task dir so the tasks consuming this one's outputs can chain on it.
     */
    protected String cacheKey() {
        final env = new TreeMap<String,String>(task.environment ?: Collections.<String,String>emptyMap())
        final identity = [
            FlyteActionBuilder.CACHE_VERSION,
            task.processor.name,
            task.script,
            task.container,
            env.toString(),
            executor.binHash,
            executor.session.stubRun ? 'stub-run' : '',
        ] as List<String>
        final key = executor.cacheKeys.compute(task.getInputFilesMap(), identity)
        executor.cacheKeys.save(task.workDir, key)
        return key
    }

    @Override
    boolean checkIfRunning() {
        if( !actionName || !isSubmitted() )
            return false
        // null until the action becomes readable, shortly after enqueue
        final phase = executor.phaseOf(actionName)
        if( phase in STARTED_PHASES ) {
            status = TaskStatus.RUNNING
            return true
        }
        return false
    }

    @Override
    boolean checkIfCompleted() {
        if( !isRunning() )
            return false
        final phase = executor.phaseOf(actionName)
        if( !(phase in FlyteClient.TERMINAL_PHASES) )
            return false

        task.stdout = outputFile
        task.stderr = errorFile
        if( phase == 'ACTION_PHASE_SUCCEEDED' )
            completeSucceeded()
        else
            completeFailed(phase)
        status = TaskStatus.COMPLETED
        return true
    }

    /**
     * The action's `task_dir` output holds the task's outputs and `.command.*` files: copy it
     * into the Nextflow work dir, where Nextflow collects outputs and the exit status.
     */
    private void completeSucceeded() {
        final taskDir = taskDirOutput()
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

    private String taskDirOutput() {
        try {
            final data = executor.client.getActionData(FlyteActionBuilder.actionId(executor.flyteConfig, actionName))
            return FlyteClient.blobUri(data?.outputs as Map, FlyteTaskLauncher.TASK_DIR_OUTPUT)
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
                if( dest.parent )
                    Files.createDirectories(dest.parent)
                Files.copy(file, dest, StandardCopyOption.REPLACE_EXISTING)
            }
        }
    }

    private void completeFailed(String phase) {
        final message = failureMessage(phase)
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

    private String failureMessage(String phase) {
        try {
            final details = executor.client.getActionDetails(FlyteActionBuilder.actionId(executor.flyteConfig, actionName))
            final error = details?.errorInfo as Map
            if( error?.message )
                return error.message as String
            final abort = details?.abortInfo as Map
            if( abort?.reason )
                return "aborted: ${abort.reason}"
        }
        catch( Exception e ) {
            log.debug "[FLYTE] Cannot fetch details for action $actionName | ${e.message}"
        }
        return phase
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
        if( !actionName )
            return
        try {
            executor.client.abort(FlyteActionBuilder.actionId(executor.flyteConfig, actionName), 'Killed by Nextflow', executor.flyteConfig.parentAction)
        }
        catch( NotFoundException e ) {
            log.debug "[FLYTE] Action $actionName not found while killing it"
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
