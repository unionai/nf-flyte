package unionai.plugin.executor

import groovy.transform.CompileStatic
import nextflow.util.Duration
import nextflow.util.MemoryUnit
import unionai.plugin.config.FlyteConfig

/**
 * Builds the JSON-shaped `flyteidl2.actions.Action` for a Nextflow task: a raw container task
 * whose inputs and outputs Flyte's copilot moves. Every staged input is a File or Dir input, and
 * the task's work dir comes back as the `task_dir` Dir output. Flyte retries stay off, because
 * Nextflow's `errorStrategy` owns them; Flyte caching is keyed on the Nextflow task hash.
 */
@CompileStatic
class FlyteActionBuilder {

    static final String TASK_VERSION = 'nextflow'
    /** Bump when the task layout changes in a way that invalidates cached results */
    static final String CACHE_VERSION = 'nf-flyte-1'

    private final FlyteConfig config

    String name
    String shortName
    String group
    String image
    Integer cpus
    MemoryUnit memory
    MemoryUnit disk
    Integer gpus
    Duration time
    Map<String,String> env = [:]
    /** Location of the task's encoded `flyteidl2.task.Inputs` */
    String inputsUri
    /** Input name to whether it is a directory (Dir) rather than a file (File) */
    Map<String,Boolean> inputs = [:]
    /** When set, Flyte caches the task's result under this key */
    String cacheKey

    FlyteActionBuilder(FlyteConfig config) {
        this.config = config
    }

    Map build() {
        final action = [
            actionId: actionId(config, name),
            parentActionName: config.parentAction,
            inputUri: inputsUri,
            runOutputBase: config.runOutputBase,
            task: taskAction(),
        ] as Map<String,Object>
        if( group )
            action.group = group
        return action
    }

    private Map taskAction() {
        final result = [spec: [taskTemplate: taskTemplate(), shortName: truncate(shortName, 63)]] as Map<String,Object>
        if( config.queue )
            result.queue = config.queue
        if( cacheKey )
            result.cacheKey = cacheKey
        return result
    }

    private Map taskTemplate() {
        final metadata = [
            runtime: [type: 'OTHER', flavor: 'nextflow'],
            retries: [retries: 0],
        ] as Map<String,Object>
        if( time )
            metadata.timeout = "${time.toSeconds()}s".toString()
        if( cacheKey ) {
            metadata.discoverable = true
            metadata.discoveryVersion = CACHE_VERSION
        }

        return [
            id: [
                resourceType: 'TASK',
                org: config.org,
                project: config.project,
                domain: config.domain,
                name: truncate(shortName, 255),
                version: TASK_VERSION,
            ],
            type: 'raw-container',
            metadata: metadata,
            interface: [
                inputs: [variables: inputs.collect { name, dir -> variable(name, dir) }],
                outputs: [variables: [variable(FlyteTaskLauncher.TASK_DIR_OUTPUT, true)]],
            ],
            container: container(),
        ]
    }

    private static Map variable(String name, boolean dir) {
        return [key: name, value: [type: [blob: [format: '', dimensionality: dir ? 'MULTIPART' : 'SINGLE']]]]
    }

    private Map container() {
        final result = [
            image: image,
            command: ['bash', '-c', FlyteTaskLauncher.ENTRYPOINT],
            dataConfig: [
                enabled: true,
                inputPath: FlyteTaskLauncher.POD_INPUTS,
                outputPath: FlyteTaskLauncher.POD_OUTPUTS,
                format: 'JSON',
                ioStrategy: [downloadMode: 'DOWNLOAD_EAGER', uploadMode: 'UPLOAD_ON_EXIT'],
                // each File keeps its original name, in its own directory
                fileInputLayout: 'NAMED_DIR',
            ],
        ] as Map<String,Object>
        final resources = resources()
        if( resources )
            result.resources = [requests: resources, limits: resources]
        if( env )
            result.env = env.collect { k, v -> [key: k, value: v] }
        return result
    }

    private List<Map> resources() {
        final result = new ArrayList<Map>()
        if( cpus )
            result << [name: 'CPU', value: cpus.toString()]
        if( memory )
            result << [name: 'MEMORY', value: "${memory.toMega()}Mi".toString()]
        if( gpus )
            result << [name: 'GPU', value: gpus.toString()]
        if( disk )
            result << [name: 'EPHEMERAL_STORAGE', value: "${disk.toMega()}Mi".toString()]
        return result
    }

    static Map runId(FlyteConfig config) {
        return [org: config.org, project: config.project, domain: config.domain, name: config.runName]
    }

    static Map actionId(FlyteConfig config, String name) {
        return [run: runId(config), name: name]
    }

    /**
     * Flyte action names must be short and DNS-friendly; derive a deterministic one from the
     * task hash, which is unique per task attempt (each attempt has its own work dir).
     */
    static String actionName(String taskHash) {
        return 'nf-' + taskHash.toLowerCase().replaceAll(/[^a-z0-9]/, '').take(24)
    }

    /**
     * Short, UI-friendly name from the process name, e.g. `NFCORE_RNASEQ:RNASEQ:FASTQC` -> `nf.FASTQC`.
     */
    static String shortName(String processName) {
        final simple = processName?.tokenize(':')?.last() ?: 'task'
        return 'nf.' + simple.replaceAll(/[^A-Za-z0-9_.-]/, '_')
    }

    private static String truncate(String s, int max) {
        s && s.length() > max ? s.substring(0, max) : s
    }
}
