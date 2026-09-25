package unionai.plugin.executor

import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
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
import unionai.plugin.client.FlyteClient
import unionai.plugin.config.FlyteConfig

/**
 * Runs each Nextflow task as a child action of the Flyte v2 action running the Nextflow
 * head (by default the root action `a0`).
 *
 * Each task is a raw container action whose data Flyte's copilot moves: the task's inputs
 * go in as File / Dir inputs and its work dir comes back as a Dir output (see
 * {@link FlyteTaskLauncher}). Task pods never access the work dir themselves, so it can be on
 * any object store that both Nextflow and Flyte can read.
 */
@Slf4j
@ServiceName('flyte')
@CompileStatic
class FlyteExecutor extends Executor implements ExtensionPoint {

    private FlyteConfig flyteConfig
    private FlyteClient client
    private Path remoteBinDir
    private FlyteCacheKey cacheKeys
    private StandaloneRun standaloneRun
    private String binHash = ''

    // Status of every action in the run, refreshed at most once per poll interval
    // with a single paged ListActions call shared by all task handlers.
    private volatile Map<String,String> phases = Collections.emptyMap()
    private volatile long phasesRefreshedAt = 0

    FlyteConfig getFlyteConfig() { flyteConfig }
    FlyteClient getClient() { client }
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
        createClient()
        cacheKeys = new FlyteCacheKey(getWorkDir())
        if( flyteConfig.standalone )
            startStandaloneRun()
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

    protected void createClient() {
        client = new FlyteClient("https://${flyteConfig.endpoint}", flyteConfig.apiKey, flyteConfig.authCommand)
    }

    /**
     * Launched outside Flyte: create the run the task actions go into (see {@link StandaloneRun})
     */
    protected void startStandaloneRun() {
        standaloneRun = new StandaloneRun(flyteConfig, client, getWorkDir())
        standaloneRun.start()
    }

    @Override
    void shutdown() {
        if( !standaloneRun )
            return
        // the run's root action ends with the pipeline; stopped by a signal (Ctrl+C, SIGTERM,
        // SIGHUP), the run is aborted instead, as the user asked
        final error = session.error
        if( error instanceof AbortSignalException )
            standaloneRun.abort("Cancelled from Nextflow (${error.message})")
        else
            standaloneRun.finish(session.isSuccess())
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

    /**
     * @return the last known phase of the named action, refreshing the run-wide phase
     *      snapshot when it is older than the poll interval
     */
    String phaseOf(String actionName) {
        final maxAge = flyteConfig.pollInterval.toMillis()
        if( System.currentTimeMillis() - phasesRefreshedAt > maxAge )
            refreshPhases(maxAge)
        return phases.get(actionName)
    }

    @PackageScope
    synchronized void refreshPhases(long maxAge) {
        // another handler may have refreshed while this one waited on the lock
        if( System.currentTimeMillis() - phasesRefreshedAt <= maxAge )
            return
        phases = client.listActionPhases(FlyteActionBuilder.runId(flyteConfig))
        phasesRefreshedAt = System.currentTimeMillis()
        log.trace "[FLYTE] refreshed phases of ${phases.size()} actions"
    }
}
