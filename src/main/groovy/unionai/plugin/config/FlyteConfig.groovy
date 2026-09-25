package unionai.plugin.config

import groovy.transform.CompileStatic
import groovy.transform.ToString
import nextflow.Session
import nextflow.SysEnv
import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.ScopeName
import nextflow.script.dsl.Description
import nextflow.util.Duration
import unionai.plugin.client.FlyteClient

/**
 * The `flyte` config scope.
 *
 * Every option defaults to what the Flyte runtime injects into the pod of the root
 * action (`a0`) that runs the Nextflow head, so inside Flyte the scope can be left empty.
 */
@ScopeName('flyte')
@Description('''
    The `flyte` scope configures the Flyte v2 executor.
''')
@ToString(includeNames = true, includePackage = false, excludes = ['apiKey', 'authCommand'])
@CompileStatic
class FlyteConfig implements ConfigScope {

    @ConfigOption
    @Description('Flyte API endpoint host, e.g. `tenant.hosted.unionai.cloud` (default: `_U_EP_OVERRIDE`).')
    String endpoint

    @ConfigOption
    @Description('Flyte org (default: `_U_ORG_NAME`).')
    String org

    @ConfigOption
    @Description('Flyte project (default: `FLYTE_INTERNAL_EXECUTION_PROJECT`).')
    String project

    @ConfigOption
    @Description('Flyte domain (default: `FLYTE_INTERNAL_EXECUTION_DOMAIN`).')
    String domain

    @ConfigOption
    @Description('Name of the run that Nextflow tasks are added to (default: `NF_FLYTE_RUN_NAME`).')
    String runName

    @ConfigOption
    @Description('Action that task actions are parented under, i.e. the action running the Nextflow head (default: `NF_FLYTE_PARENT_ACTION`, else `a0`).')
    String parentAction

    @ConfigOption
    @Description('Object store prefix for the run\'s action outputs (default: `_U_RUN_BASE`).')
    String runOutputBase

    @ConfigOption
    @Description('Flyte queue to submit task actions to (default: the run\'s queue).')
    String queue

    @ConfigOption
    @Description('How often to poll Flyte for task status (default: `5s`).')
    Duration pollInterval

    @ConfigOption
    @Description('Cache task results in Flyte, keyed on the Nextflow task hash, so a task with the same hash is not run again, in this run or another (default: `true`). Processes with `cache false` are never cached.')
    boolean cache

    @ConfigOption
    @Description('Command that prints a Flyte access token, as an alternative to an API key (e.g. a CLI that logs you in). It runs again whenever the token is rejected.')
    String authCommand

    @ConfigOption
    @Description('When Nextflow is launched outside Flyte: how long the run waits without a heartbeat from the Nextflow head before failing, which also aborts its tasks (default: `5m`).')
    Duration heartbeatTimeout

    @ConfigOption
    @Description('When Nextflow is launched outside Flyte: image of the run\'s root action, which only waits for the Nextflow head to finish (default: the Flyte runtime image).')
    String rootImage

    /* API key: base64("endpoint:client_id:client_secret:org"), never read from the config file */
    String apiKey

    /* required by extension point -- do not remove */
    FlyteConfig() {}

    FlyteConfig(Map opts, Map<String,String> env) {
        endpoint = stripScheme(opts.endpoint as String ?: env._U_EP_OVERRIDE ?: env.FLYTE_ADMIN_ENDPOINT)
        org = opts.org as String ?: env._U_ORG_NAME
        project = opts.project as String ?: env.FLYTE_INTERNAL_EXECUTION_PROJECT
        domain = opts.domain as String ?: env.FLYTE_INTERNAL_EXECUTION_DOMAIN
        runName = opts.runName as String ?: env.NF_FLYTE_RUN_NAME
        parentAction = opts.parentAction as String ?: env.NF_FLYTE_PARENT_ACTION ?: 'a0'
        runOutputBase = opts.runOutputBase as String ?: env._U_RUN_BASE
        queue = opts.queue as String
        pollInterval = opts.pollInterval ? opts.pollInterval as Duration : Duration.of('5s')
        cache = opts.cache != null ? opts.cache as boolean : true
        apiKey = env._UNION_EAGER_API_KEY ?: env.EAGER_API_KEY ?: env.FLYTE_API_KEY
        authCommand = opts.authCommand as String ?: env.FLYTE_AUTH_COMMAND
        heartbeatTimeout = opts.heartbeatTimeout ? opts.heartbeatTimeout as Duration : Duration.of('5m')
        rootImage = opts.rootImage as String ?: DEFAULT_ROOT_IMAGE
        // an API key also names its endpoint and org
        if( apiKey && (!endpoint || !org) ) {
            try {
                final key = FlyteClient.decodeApiKey(apiKey)
                endpoint = endpoint ?: stripScheme(key.endpoint)
                org = org ?: key.org
            }
            catch( Exception e ) {
                // reported when the client authenticates
            }
        }
    }

    static final String DEFAULT_ROOT_IMAGE = 'ghcr.io/flyteorg/flyte:py3.12-v2.10.0'

    /**
     * @return whether Nextflow was launched outside Flyte (e.g. `nextflow run` on a laptop or in
     *      CI), in which case the executor creates the Flyte run itself. Inside Flyte, the task
     *      that runs Nextflow names the run its task actions belong to.
     */
    boolean isStandalone() {
        return !runName
    }

    static FlyteConfig create(Session session) {
        return new FlyteConfig(session.config.flyte as Map ?: Collections.emptyMap(), SysEnv.get())
    }

    /**
     * @return the names of required settings that could not be resolved
     */
    List<String> missing() {
        final result = new ArrayList<String>()
        if( !endpoint ) result << 'endpoint'
        if( !org ) result << 'org'
        if( !project ) result << 'project'
        if( !domain ) result << 'domain'
        if( !apiKey && !authCommand ) result << 'apiKey (env FLYTE_API_KEY) or authCommand'
        // inside Flyte the run already exists; launched outside, the executor creates it
        if( !standalone && !runOutputBase ) result << 'runOutputBase'
        return result
    }

    static protected String stripScheme(String endpoint) {
        return endpoint?.replaceFirst(/^(dns:\/\/\/|https?:\/\/)/, '')?.replaceFirst(/\/+$/, '')
    }
}
