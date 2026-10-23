package unionai.plugin.executor

import com.google.protobuf.StringValue
import flyteidl2.actions.ActionsServiceOuterClass.Action
import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.common.Identifier.RunIdentifier
import flyteidl2.core.IdentifierOuterClass.Identifier
import flyteidl2.core.IdentifierOuterClass.ResourceType
import flyteidl2.core.Interface.TypedInterface
import flyteidl2.core.Interface.Variable
import flyteidl2.core.Interface.VariableEntry
import flyteidl2.core.Interface.VariableMap
import flyteidl2.core.Literals.KeyValuePair
import flyteidl2.core.Literals.RetryStrategy
import flyteidl2.core.Tasks.Container
import flyteidl2.core.Tasks.DataLoadingConfig
import flyteidl2.core.Tasks.IOStrategy
import flyteidl2.core.Tasks.Resources
import flyteidl2.core.Tasks.Resources.ResourceEntry
import flyteidl2.core.Tasks.Resources.ResourceName
import flyteidl2.core.Tasks.RuntimeMetadata
import flyteidl2.core.Tasks.TaskMetadata
import flyteidl2.core.Tasks.TaskTemplate
import flyteidl2.core.Types.BlobType
import flyteidl2.core.Types.LiteralType
import flyteidl2.task.TaskDefinition.TaskIdentifier
import flyteidl2.task.TaskDefinition.TaskSpec
import flyteidl2.workflow.RunDefinition.TaskAction
import groovy.transform.CompileStatic
import nextflow.util.Duration
import nextflow.util.MemoryUnit
import unionai.plugin.config.FlyteConfig

/**
 * Builds the `flyteidl2.actions.Action` for a Nextflow task: a raw container task
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

    Action build() {
        final action = Action.newBuilder()
            .setActionId(actionId(config, name))
            .setParentActionName(config.parentAction)
            .setInputUri(inputsUri)
            .setRunOutputBase(config.runOutputBase)
            .setTask(taskAction())
        if( group )
            action.setGroup(group)
        return action.build()
    }

    private TaskAction taskAction() {
        final template = taskTemplate()
        final tid = template.id
        final result = TaskAction.newBuilder()
            .setId(TaskIdentifier.newBuilder().setOrg(tid.org).setProject(tid.project).setDomain(tid.domain).setName(tid.name).setVersion(tid.version))
            .setSpec(TaskSpec.newBuilder().setTaskTemplate(template).setShortName(truncate(shortName, 63)))
        if( config.queue )
            result.setQueue(config.queue)
        if( cacheKey )
            result.setCacheKey(StringValue.of(cacheKey))
        return result.build()
    }

    private TaskTemplate taskTemplate() {
        final metadata = TaskMetadata.newBuilder()
            .setRuntime(RuntimeMetadata.newBuilder().setType(RuntimeMetadata.RuntimeType.OTHER).setFlavor('nextflow'))
            .setRetries(RetryStrategy.newBuilder().setRetries(0))
        if( time )
            metadata.setTimeout(com.google.protobuf.Duration.newBuilder().setSeconds(time.toSeconds()))
        if( cacheKey ) {
            metadata.setDiscoverable(true)
            metadata.setDiscoveryVersion(CACHE_VERSION)
        }

        final inputVars = VariableMap.newBuilder()
        inputs.each { String name, Boolean dir -> inputVars.addVariables(variable(name, dir)) }

        return TaskTemplate.newBuilder()
            .setId(Identifier.newBuilder()
                .setResourceType(ResourceType.TASK)
                .setOrg(config.org)
                .setProject(config.project)
                .setDomain(config.domain)
                .setName(truncate(shortName, 255))
                .setVersion(TASK_VERSION))
            .setType('raw-container')
            .setMetadata(metadata)
            .setInterface(TypedInterface.newBuilder()
                .setInputs(inputVars)
                .setOutputs(VariableMap.newBuilder().addVariables(variable(FlyteTaskLauncher.TASK_DIR_OUTPUT, true))))
            .setContainer(container())
            .build()
    }

    private static VariableEntry variable(String name, boolean dir) {
        final blob = BlobType.newBuilder()
            .setDimensionality(dir ? BlobType.BlobDimensionality.MULTIPART : BlobType.BlobDimensionality.SINGLE)
        return VariableEntry.newBuilder()
            .setKey(name)
            .setValue(Variable.newBuilder().setType(LiteralType.newBuilder().setBlob(blob)))
            .build()
    }

    private Container container() {
        final result = Container.newBuilder()
            .setImage(image)
            .addAllCommand(['bash', '-c', FlyteTaskLauncher.ENTRYPOINT])
            .setDataConfig(DataLoadingConfig.newBuilder()
                .setEnabled(true)
                .setInputPath(FlyteTaskLauncher.POD_INPUTS)
                .setOutputPath(FlyteTaskLauncher.POD_OUTPUTS)
                .setFormat(DataLoadingConfig.LiteralMapFormat.JSON)
                .setIoStrategy(IOStrategy.newBuilder()
                    .setDownloadMode(IOStrategy.DownloadMode.DOWNLOAD_EAGER)
                    .setUploadMode(IOStrategy.UploadMode.UPLOAD_ON_EXIT))
                // each File keeps its original name, in its own directory
                .setFileInputLayout(DataLoadingConfig.FileInputLayout.NAMED_DIR))
        final resources = resources()
        if( resources )
            result.setResources(Resources.newBuilder().addAllRequests(resources).addAllLimits(resources))
        env.each { String k, String v -> result.addEnv(KeyValuePair.newBuilder().setKey(k).setValue(v)) }
        return result.build()
    }

    private List<ResourceEntry> resources() {
        final result = new ArrayList<ResourceEntry>()
        if( cpus )
            result << resource(ResourceName.CPU, cpus.toString())
        if( memory )
            result << resource(ResourceName.MEMORY, "${memory.toMega()}Mi".toString())
        if( gpus )
            result << resource(ResourceName.GPU, gpus.toString())
        if( disk )
            result << resource(ResourceName.EPHEMERAL_STORAGE, "${disk.toMega()}Mi".toString())
        return result
    }

    private static ResourceEntry resource(ResourceName name, String value) {
        ResourceEntry.newBuilder().setName(name).setValue(value).build()
    }

    static RunIdentifier runId(FlyteConfig config) {
        return RunIdentifier.newBuilder()
            .setOrg(config.org)
            .setProject(config.project)
            .setDomain(config.domain)
            .setName(config.runName)
            .build()
    }

    static ActionIdentifier actionId(FlyteConfig config, String name) {
        return ActionIdentifier.newBuilder().setRun(runId(config)).setName(name).build()
    }

    /**
     * Flyte action names must be short and DNS-friendly; derive a deterministic one from the
     * task hash, which is unique per task attempt (each attempt has its own work dir).
     */
    static String actionName(String taskHash) {
        return 'nf-' + taskHash.toLowerCase().replaceAll(/[^a-z0-9]/, '').take(24)
    }

    /**
     * Action name from a task's cache key and Nextflow attempt (`failCount`), which, unlike the
     * task hash, stay the same when a retried head resumes the task.
     */
    static String actionName(String cacheKey, int failCount) {
        return actionName(cacheKey) + '-' + failCount
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
