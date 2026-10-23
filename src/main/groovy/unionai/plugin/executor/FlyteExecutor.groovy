package unionai.plugin.executor

import java.nio.file.Path
import java.time.Duration

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException
import nextflow.exception.AbortSignalException
import nextflow.executor.Executor
import nextflow.extension.FilesEx
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.util.ServiceName
import org.pf4j.ExtensionPoint
import unionai.flyte.controller.ActionController
import unionai.flyte.controller.ActionListener
import unionai.flyte.controller.ActionState
import unionai.flyte.controller.FlyteConnection
import unionai.flyte.controller.RunClient
import unionai.plugin.config.FlyteConfig

/**
 * Runs each Nextflow task as a child action of the Flyte v2 action running the Nextflow
 * head (by default the root action `a0`).
 *
 * Each task is a raw container action whose data Flyte's copilot moves: the task's inputs
 * go in as File / Dir inputs and its work dir comes back as a Dir output (see
 * {@link FlyteTaskLauncher}). Task pods never access the work dir themselves, so it can be on
 * any object store that both Nextflow and Flyte can read.
 *
 * Task actions are enqueued through the Actions service, and their state comes from its watch of
 * the parent action's children (see {@link ActionController}): task handlers read it from memory,
 * and every change wakes the task monitor, so nothing polls Flyte.
 */
@Slf4j
@ServiceName('flyte')
@CompileStatic
class FlyteExecutor extends Executor implements ExtensionPoint {

    /** How long to wait for the watch to list the parent's existing children */
    static final Duration WATCH_SYNC_TIMEOUT = Duration.ofMinutes(2)

    private FlyteConfig flyteConfig
    private FlyteConnection connection
    private ActionController controller
    private Path remoteBinDir
    private FlyteCacheKey cacheKeys
    private StandaloneRun standaloneRun
    private String binHash = ''
    /** Set once the run's storage proved unwritable: task inputs then go to their work dirs */
    volatile boolean inputsInWorkDir

    FlyteConfig getFlyteConfig() { flyteConfig }
    ActionController getController() { controller }
    RunClient getRuns() { connection.runs }
    Path getRemoteBinDir() { remoteBinDir }
    FlyteCacheKey getCacheKeys() { cacheKeys }
    String getBinHash() { binHash }

    @Override
    final boolean isContainerNative() {
        return true
    }

    @Override
    String containerConfigEngine() {
        return 'docker'
    }

    @Override
    final Path getWorkDir() {
        return session.bucketDir ?: session.workDir
    }

    @Override
    protected void register() {
        super.register()
        createConfig()
        validateWorkDir()
        uploadBinDir()
        createConnection()
        cacheKeys = new FlyteCacheKey(getWorkDir())
        if( flyteConfig.standalone )
            startStandaloneRun()
        startController()
    }

    protected void createConfig() {
        flyteConfig = FlyteConfig.create(session)
        final missing = flyteConfig.missing()
        if( missing ) {
            session.abort()
            throw new AbortOperationException("Executor `flyte` is missing required settings: ${missing.join(', ')} -- set them in the `flyte` config scope, or run Nextflow inside a Flyte task (see the nf-flyte README)")
        }
        log.debug "[FLYTE] Executor config=$flyteConfig"
    }

    protected void validateWorkDir() {
        final scheme = getWorkDir()?.scheme
        if( !scheme || scheme == 'file' ) {
            session.abort()
            throw new AbortOperationException("Executor `flyte` requires an object store work directory that Flyte can read, e.g. `-w s3://<bucket>/<path>` -- add it to the command line or set `workDir` in the config")
        }
    }

    protected void uploadBinDir() {
        if( session.binDir && !session.binDir.empty() && !session.disableRemoteBinDir ) {
            final tmp = getTempDir()
            log.info "Uploading local `bin` scripts folder to ${tmp.toUriString()}/bin"
            remoteBinDir = FilesEx.copyTo(session.binDir, tmp)
            binHash = FlyteCacheKey.binHash(session.binDir)
        }
    }

    protected void createConnection() {
        final builder = FlyteConnection.builder(flyteConfig.endpoint)
        connection = (flyteConfig.authCommand ? builder.authCommand(flyteConfig.authCommand) : builder.apiKey(flyteConfig.apiKey)).build()
    }

    /**
     * Launched outside Flyte: create the run the task actions go into (see {@link StandaloneRun})
     */
    protected void startStandaloneRun() {
        standaloneRun = new StandaloneRun(flyteConfig, connection.runs, getWorkDir())
        standaloneRun.start()
    }

    /**
     * Watch the task actions, i.e. the children of the action running the Nextflow head
     */
    protected void startController() {
        controller = connection.controller(FlyteActionBuilder.runId(flyteConfig), flyteConfig.parentAction)
        controller.addListener(new ActionListener() {
            @Override
            void onUpdate(ActionState state) {
                // a task started or finished: let the monitor check it now, not at the next poll
                if( state.started )
                    signalMonitor()
            }

            @Override
            void onWatchFailed(Throwable error) {
                // the tasks' next check fails with the error
                signalMonitor()
            }
        })
        controller.start(WATCH_SYNC_TIMEOUT)
    }

    private void signalMonitor() {
        final monitor = getMonitor()
        if( monitor instanceof TaskPollingMonitor )
            ((TaskPollingMonitor) monitor).signal()
    }

    @Override
    void shutdown() {
        try {
            if( standaloneRun ) {
                // the run's root action ends with the pipeline; stopped by a signal (Ctrl+C,
                // SIGTERM, SIGHUP), the run is aborted instead, as the user asked
                final error = session.error
                if( error instanceof AbortSignalException )
                    standaloneRun.abort("Cancelled from Nextflow (${error.message})")
                else
                    standaloneRun.finish(session.isSuccess())
            }
        }
        finally {
            // waits briefly for the aborts of killed tasks
            controller?.close()
            connection?.close()
        }
    }

    @Override
    protected TaskMonitor createTaskMonitor() {
        // runs before register(), so read the poll interval straight from the session config
        TaskPollingMonitor.create(session, config, name, 1000, FlyteConfig.create(session).pollInterval)
    }

    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        return new FlyteTaskHandler(task, this)
    }
}
