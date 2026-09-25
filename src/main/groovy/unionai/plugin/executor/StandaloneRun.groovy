package unionai.plugin.executor

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.extension.FilesEx
import unionai.plugin.client.FlyteClient
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

    /**
     * The root action's program (Python, in the Flyte runtime image, whose storage layer reads
     * any object store with the pod's credentials). Args: status file, heartbeat file, timeout.
     */
    static final String WATCHER = '''\
        import sys, time

        status, heartbeat, timeout = sys.argv[1], sys.argv[2], float(sys.argv[3])
        if "://" in status:
            import flyte.storage
            fs = flyte.storage.get_underlying_filesystem(path=status)
            exists, read = fs.exists, lambda p: fs.cat_file(p).decode()
        else:
            import os
            exists, read = os.path.exists, lambda p: open(p).read()

        started = time.time()
        print("Waiting for the Nextflow head to finish", flush=True)
        while True:
            if exists(status):
                result = read(status).strip()
                print(f"Nextflow head reported: {result}", flush=True)
                sys.exit(0 if result == "succeeded" else 1)
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
    private final FlyteClient client
    private final Path markerDir
    private ScheduledExecutorService heartbeats

    @PackageScope String runName
    private Map runId

    StandaloneRun(FlyteConfig config, FlyteClient client, Path workDir) {
        this.config = config
        this.client = client
        this.markerDir = workDir.resolve(".flyte/heads/${UUID.randomUUID()}")
    }

    Path getStatusFile() { markerDir.resolve('status') }

    Path getHeartbeatFile() { markerDir.resolve('heartbeat') }

    /**
     * Create the run, point the config at it and start the heartbeat.
     */
    void start() {
        Files.createDirectories(markerDir)
        beat()
        final run = client.createRun([
            projectId: [organization: config.org, name: config.project, domain: config.domain],
            taskSpec: rootSpec(),
        ])
        final id = ((run.action as Map).id as Map).run as Map
        runId = id
        runName = id.name as String
        config.runName = runName
        config.parentAction = HEAD_ACTION
        config.runOutputBase = runOutputBase(client.getActionDataUris([run: id, name: HEAD_ACTION]))

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
            client.abortRun(runId, reason)
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
    static String runOutputBase(Map uris) {
        final inputs = uris?.inputsUri as String
        final marker = "/${HEAD_ACTION}/"
        if( !inputs || !inputs.contains(marker) )
            throw new IllegalStateException("Unexpected storage location for the run's root action: $inputs")
        return inputs.substring(0, inputs.lastIndexOf(marker))
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

    protected Map rootSpec() {
        return [
            shortName: 'nextflow',
            taskTemplate: [
                id: [resourceType: 'TASK', org: config.org, project: config.project, domain: config.domain, name: 'nf.head', version: FlyteActionBuilder.TASK_VERSION],
                type: 'container',
                metadata: [
                    // the control plane only sends a run through the Actions service, which task
                    // actions are enqueued with, when its root declares a v2 runtime version
                    runtime: [type: 'OTHER', flavor: 'nextflow', version: '2.10.0'],
                    retries: [retries: 0],
                ],
                interface: [:],
                container: [
                    image: config.rootImage,
                    command: ['python', '-c', WATCHER, FilesEx.toUriString(statusFile), FilesEx.toUriString(heartbeatFile), config.heartbeatTimeout.toSeconds().toString()],
                    resources: [
                        requests: [[name: 'CPU', value: '100m'], [name: 'MEMORY', value: '256Mi']],
                        limits: [[name: 'CPU', value: '500m'], [name: 'MEMORY', value: '512Mi']],
                    ],
                ],
            ],
        ]
    }
}
