package unionai.plugin.executor

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.common.Identifier.RunIdentifier
import flyteidl2.core.IdentifierOuterClass.Identifier
import flyteidl2.core.IdentifierOuterClass.ResourceType
import flyteidl2.core.Interface.TypedInterface
import flyteidl2.core.Literals.RetryStrategy
import flyteidl2.core.Tasks.Container
import flyteidl2.core.Tasks.Resources
import flyteidl2.core.Tasks.Resources.ResourceName
import flyteidl2.core.Tasks.RuntimeMetadata
import flyteidl2.core.Tasks.TaskMetadata
import flyteidl2.core.Tasks.TaskTemplate
import flyteidl2.dataproxy.DataproxyService.UploadInputsRequest
import flyteidl2.task.Common.Inputs
import flyteidl2.task.TaskDefinition.TaskSpec
import flyteidl2.workflow.RunServiceOuterClass.CreateRunRequest
import flyteidl2.workflow.RunServiceOuterClass.GetActionDataURIsResponse
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.extension.FilesEx
import unionai.flyte.controller.RunClient
import unionai.plugin.config.FlyteConfig

/**
 * The Flyte run of a pipeline launched outside Flyte (`nextflow run` on a laptop, in CI, ...).
 *
 * Task actions need a parent action in a run. Here there is no Flyte task running Nextflow,
 * so the executor creates a run whose root action stands in for the Nextflow head: it waits
 * until the head reports how the pipeline ended, and then succeeds or fails with it. The head
 * also writes a heartbeat; if it stops (laptop closed, CI job killed), the root fails, and Flyte
 * aborts the run's remaining task actions. Both files live in the work dir, which Flyte can read.
 */
@Slf4j
@CompileStatic
class StandaloneRun {

    static final String HEAD_ACTION = 'a0'
    static final long HEARTBEAT_SECONDS = 30
    static final String SUCCEEDED = 'succeeded'
    static final String FAILED = 'failed'
    static final String ABORTED = 'aborted'
    /** Replaced by Flyte with where the root action's outputs go */
    static final String OUTPUT_PREFIX = '{{.outputPrefix}}'

    /**
     * The root action's program (Python, in the Flyte runtime image, whose storage layer reads
     * any object store with the pod's credentials). Args: status file, heartbeat file, timeout,
     * and the action's output prefix (templated by Flyte), where it writes its empty outputs.
     */
    static final String WATCHER = '''\
        import sys, time

        status, heartbeat, timeout = sys.argv[1], sys.argv[2], float(sys.argv[3])
        outputs = sys.argv[4].rstrip("/") + "/outputs.pb" if len(sys.argv) > 4 else None

        def fs_for(path):
            if "://" in path:
                import flyte.storage
                fs = flyte.storage.get_underlying_filesystem(path=path)
                return fs.exists, lambda p: fs.cat_file(p).decode(), lambda p, b: fs.pipe_file(p, b)
            import os
            def write(p, b):
                os.makedirs(os.path.dirname(p), exist_ok=True)
                open(p, "wb").write(b)
            return os.path.exists, lambda p: open(p).read(), write

        exists, read, _ = fs_for(status)

        def succeed():
            # the root has no outputs: an empty outputs.pb tells Flyte (and its UI) so
            if outputs:
                fs_for(outputs)[2](outputs, b"")
            sys.exit(0)

        started = time.time()
        print("Waiting for the Nextflow head to finish", flush=True)
        while True:
            if exists(status):
                result = read(status).strip()
                print(f"Nextflow head reported: {result}", flush=True)
                if result == "succeeded":
                    succeed()
                sys.exit(1)
            try:
                last = float(read(heartbeat).strip())
            except Exception:
                last = started
            if time.time() - max(last, started) > timeout:
                print(f"No heartbeat from the Nextflow head for {timeout:.0f}s: failing the run, which aborts its tasks", flush=True)
                sys.exit(1)
            time.sleep(10)
        '''.stripIndent()

    private final FlyteConfig config
    private final RunClient runs
    private final Path markerDir
    private final String pipelineName
    private ScheduledExecutorService heartbeats

    @PackageScope String runName
    private RunIdentifier runId

    /**
     * @param pipelineName what the root's task is named after, e.g. `nf-core/rnaseq`
     */
    StandaloneRun(FlyteConfig config, RunClient runs, Path workDir, String pipelineName = null) {
        this.config = config
        this.runs = runs
        this.markerDir = workDir.resolve(".flyte/heads/${UUID.randomUUID()}")
        this.pipelineName = pipelineName ?: 'nextflow'
    }

    Path getStatusFile() { markerDir.resolve('status') }

    Path getHeartbeatFile() { markerDir.resolve('heartbeat') }

    /**
     * Create the run, point the config at it and start the heartbeat.
     */
    void start() {
        Files.createDirectories(markerDir)
        beat()
        final id = RunIdentifier.newBuilder()
            .setOrg(config.org).setProject(config.project).setDomain(config.domain).setName(newRunName())
            .build()
        final spec = rootSpec()
        // the root has no inputs: store empty ones under the run, as the Flyte SDK does, so the
        // run has inputs to show (CreateRun stores none for an empty inline Inputs)
        final inputs = runs.uploadInputs(UploadInputsRequest.newBuilder()
            .setRunId(id)
            .setTaskSpec(spec)
            .setInputs(Inputs.getDefaultInstance())
            .build())
        final run = runs.createRun(CreateRunRequest.newBuilder()
            .setRunId(id)
            .setTaskSpec(spec)
            .setOffloadedInputData(inputs)
            .build())
        // null when a retried CreateRun found the run its first attempt created
        runId = run ? run.action.id.run : id
        runName = runId.name
        config.runName = runName
        config.parentAction = HEAD_ACTION
        config.runOutputBase = runOutputBase(runs.getActionDataUris(ActionIdentifier.newBuilder().setRun(runId).setName(HEAD_ACTION).build()))

        heartbeats = Executors.newSingleThreadScheduledExecutor { Runnable r ->
            final t = new Thread(r, 'flyte-heartbeat')
            t.daemon = true
            return t
        }
        heartbeats.scheduleAtFixedRate(this.&safeBeat, HEARTBEAT_SECONDS, HEARTBEAT_SECONDS, TimeUnit.SECONDS)
        log.info "Flyte run: ${url()}"
    }

    /**
     * Report how the pipeline ended; the root action exits with it.
     */
    void finish(boolean success) {
        heartbeats?.shutdownNow()
        Files.write(statusFile, (success ? SUCCEEDED : FAILED).getBytes(StandardCharsets.UTF_8))
        log.info "Flyte run ${runName} ${success ? SUCCEEDED : FAILED}: ${url()}"
    }

    /**
     * The user stopped Nextflow (Ctrl+C, SIGTERM): abort the Flyte run, so it ends ABORTED with the
     * given reason rather than FAILED. The status file is a fallback for when the abort call fails:
     * the root then ends the run itself instead of waiting for the heartbeat timeout.
     */
    void abort(String reason) {
        heartbeats?.shutdownNow()
        try {
            runs.abortRun(runId, reason)
            log.info "Flyte run ${runName} aborted: ${url()}"
        }
        catch( Exception e ) {
            log.warn "Unable to abort Flyte run ${runName}, it will end as failed -- ${e.message}"
        }
        Files.write(statusFile, ABORTED.getBytes(StandardCharsets.UTF_8))
    }

    String url() {
        return "https://${config.endpoint}/v2/domain/${config.domain}/project/${config.project}/runs/${runName}"
    }

    /** The run's storage prefix: the root's input URI is `<prefix>/a0/inputs.pb` */
    static String runOutputBase(GetActionDataURIsResponse uris) {
        final inputs = uris?.inputsUri
        final marker = "/${HEAD_ACTION}/"
        if( !inputs || !inputs.contains(marker) )
            throw new IllegalStateException("Unexpected storage location for the run's root action: $inputs")
        return inputs.substring(0, inputs.lastIndexOf(marker))
    }

    private static final String NAME_CHARS = 'abcdefghijklmnopqrstuvwxyz0123456789'

    /**
     * A run name like the ones Flyte generates: 20 lowercase letters and digits, here starting
     * with `nf`. The run's inputs are stored under it before the run exists.
     */
    static String newRunName() {
        final random = new java.security.SecureRandom()
        final result = new StringBuilder('nf')
        while( result.length() < 20 )
            result.append(NAME_CHARS.charAt(random.nextInt(NAME_CHARS.length())))
        return result.toString()
    }

    private void beat() {
        Files.write(heartbeatFile, (System.currentTimeMillis() / 1000).toString().getBytes(StandardCharsets.UTF_8))
    }

    private void safeBeat() {
        try {
            beat()
        }
        catch( Exception e ) {
            log.debug "[FLYTE] heartbeat failed | ${e.message}"
        }
    }

    protected TaskSpec rootSpec() {
        final template = TaskTemplate.newBuilder()
            .setId(Identifier.newBuilder()
                .setResourceType(ResourceType.TASK)
                .setOrg(config.org)
                .setProject(config.project)
                .setDomain(config.domain)
                .setName(taskName(pipelineName))
                .setVersion(FlyteActionBuilder.TASK_VERSION))
            .setType('container')
            .setMetadata(TaskMetadata.newBuilder()
                // the control plane only sends a run through the Actions service, which task
                // actions are enqueued with, when its root declares a v2 runtime version
                .setRuntime(RuntimeMetadata.newBuilder().setType(RuntimeMetadata.RuntimeType.OTHER).setFlavor('nextflow').setVersion('2.10.0'))
                .setRetries(RetryStrategy.newBuilder().setRetries(0)))
            .setInterface(TypedInterface.getDefaultInstance())
            .setContainer(Container.newBuilder()
                .setImage(config.rootImage)
                .addAllCommand(['python', '-c', WATCHER, FilesEx.toUriString(statusFile), FilesEx.toUriString(heartbeatFile), config.heartbeatTimeout.toSeconds().toString(), OUTPUT_PREFIX])
                .setResources(Resources.newBuilder()
                    .addRequests(FlyteActionBuilder.resource(ResourceName.CPU, '100m'))
                    .addRequests(FlyteActionBuilder.resource(ResourceName.MEMORY, '256Mi'))
                    .addLimits(FlyteActionBuilder.resource(ResourceName.CPU, '500m'))
                    .addLimits(FlyteActionBuilder.resource(ResourceName.MEMORY, '512Mi'))))
        return TaskSpec.newBuilder().setShortName(FlyteActionBuilder.truncate(pipelineName, 63)).setTaskTemplate(template).build()
    }

    /**
     * Task name of the root, from the pipeline name, e.g. `nf-core/rnaseq` -> `nf.nf-core_rnaseq`.
     * The UI shows the task's short name, which keeps the pipeline name as is.
     */
    static String taskName(String pipelineName) {
        return FlyteActionBuilder.truncate('nf.' + pipelineName.replaceAll(/[^A-Za-z0-9_.-]/, '_'), 255)
    }
}
