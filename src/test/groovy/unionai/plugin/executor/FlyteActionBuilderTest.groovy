package unionai.plugin.executor

import flyteidl2.common.Identifier.RunIdentifier
import flyteidl2.core.IdentifierOuterClass.Identifier
import flyteidl2.core.IdentifierOuterClass.ResourceType
import flyteidl2.core.Interface.VariableMap
import flyteidl2.core.Tasks.DataLoadingConfig
import flyteidl2.core.Tasks.IOStrategy
import flyteidl2.core.Tasks.Resources.ResourceEntry
import flyteidl2.core.Tasks.RuntimeMetadata
import nextflow.util.Duration
import nextflow.util.MemoryUnit
import spock.lang.Specification
import unionai.plugin.config.FlyteConfig

class FlyteActionBuilderTest extends Specification {

    static FlyteConfig config(Map opts = [:]) {
        new FlyteConfig(opts, [
            _U_EP_OVERRIDE: 'tenant.example.com',
            _U_ORG_NAME: 'acme',
            FLYTE_INTERNAL_EXECUTION_PROJECT: 'proj',
            FLYTE_INTERNAL_EXECUTION_DOMAIN: 'development',
            NF_FLYTE_RUN_NAME: 'r123',
            _U_RUN_BASE: 's3://bucket/base',
            _UNION_EAGER_API_KEY: 'x',
        ])
    }

    static FlyteActionBuilder builder(Map opts = [:]) {
        def b = new FlyteActionBuilder(config(opts))
        b.name = 'nf-abc'
        b.shortName = 'nf.FASTQC'
        b.image = 'quay.io/biocontainers/fastqc:0.12.1'
        b.inputsUri = 's3://bucket/work/ab/cdef/.command.inputs.pb'
        b.inputs = [in_0: false, in_1: true, nf_cmd_run: false, nf_cmd_sh: false, nf_bin: true]
        return b
    }

    static List<List> variables(VariableMap map) {
        map.variablesList.collect { [it.key, it.value.type.blob.format, it.value.type.blob.dimensionality.name()] }
    }

    static List<List> resources(List<ResourceEntry> entries) {
        entries.collect { [it.name.name(), it.value] }
    }

    def 'should build a copilot raw container action'() {
        given:
        def b = builder()
        b.group = 'NFCORE_RNASEQ:RNASEQ:FASTQC'
        b.cpus = 2
        b.memory = MemoryUnit.of('4 GB')
        b.time = Duration.of('1h')
        b.env = [FOO: 'bar']

        when:
        def action = b.build()

        then:
        action.actionId.run == RunIdentifier.newBuilder().setOrg('acme').setProject('proj').setDomain('development').setName('r123').build()
        action.actionId.name == 'nf-abc'
        action.parentActionName == 'a0'
        action.inputUri == 's3://bucket/work/ab/cdef/.command.inputs.pb'
        action.runOutputBase == 's3://bucket/base'
        action.group == 'NFCORE_RNASEQ:RNASEQ:FASTQC'
        !action.task.hasCacheKey()

        and:
        def tmpl = action.task.spec.taskTemplate
        action.task.spec.shortName == 'nf.FASTQC'
        tmpl.type == 'raw-container'
        tmpl.id == Identifier.newBuilder().setResourceType(ResourceType.TASK).setOrg('acme').setProject('proj').setDomain('development').setName('nf.FASTQC').setVersion('nextflow').build()
        action.task.id.name == 'nf.FASTQC'
        action.task.id.version == 'nextflow'
        tmpl.metadata.runtime.type == RuntimeMetadata.RuntimeType.OTHER
        tmpl.metadata.runtime.flavor == 'nextflow'
        tmpl.metadata.retries.retries == 0
        tmpl.metadata.timeout.seconds == 3600

        and: 'every staged input is a File or Dir input, the task dir comes back as a Dir'
        variables(tmpl.interface.inputs) == [
            ['in_0', '', 'SINGLE'],
            ['in_1', '', 'MULTIPART'],
            ['nf_cmd_run', '', 'SINGLE'],
            ['nf_cmd_sh', '', 'SINGLE'],
            ['nf_bin', '', 'MULTIPART'],
        ]
        variables(tmpl.interface.outputs) == [['task_dir', '', 'MULTIPART']]

        and: 'copilot moves the data, keeping file names'
        def c = tmpl.container
        c.image == 'quay.io/biocontainers/fastqc:0.12.1'
        c.commandCount == 0
        c.argsList == ['bash', '-c', FlyteTaskLauncher.ENTRYPOINT]
        with(c.dataConfig) {
            enabled
            inputPath == '/var/inputs'
            outputPath == '/var/outputs'
            format == DataLoadingConfig.LiteralMapFormat.JSON
            ioStrategy.downloadMode == IOStrategy.DownloadMode.DOWNLOAD_EAGER
            ioStrategy.uploadMode == IOStrategy.UploadMode.UPLOAD_ON_EXIT
            fileInputLayout == DataLoadingConfig.FileInputLayout.NAMED_DIR
        }
        c.envList.collect { [it.key, it.value] } == [['FOO', 'bar']]
        resources(c.resources.requestsList) == [['CPU', '2'], ['MEMORY', '4096Mi']]
        c.resources.limitsList == c.resources.requestsList
    }

    def 'should cache under the given key'() {
        given:
        def b = builder()
        b.cacheKey = 'a1b2c3'

        when:
        def action = b.build()

        then:
        action.task.cacheKey.value == 'a1b2c3'
        action.task.spec.taskTemplate.metadata.discoverable
        action.task.spec.taskTemplate.metadata.discoveryVersion == 'nf-flyte-1'
    }

    def 'should point the Code tab at the code bundle'() {
        given:
        def b = builder()
        b.codeBundleUri = 's3://b/work/ab/cdef/.flyte/code.tar.gz'

        expect:
        b.build().task.spec.taskTemplate.metadata.codeBundleUri == 's3://b/work/ab/cdef/.flyte/code.tar.gz'
        builder().build().task.spec.taskTemplate.metadata.codeBundleUri == ''
    }

    def 'should set the queue and omit empty settings'() {
        when:
        def action = builder(queue: 'gpu').build()

        then:
        action.task.queue == 'gpu'
        action.group == ''
        !action.task.spec.taskTemplate.container.hasResources()
        action.task.spec.taskTemplate.container.envCount == 0
        !action.task.spec.taskTemplate.metadata.hasTimeout()
        !action.task.spec.taskTemplate.metadata.discoverable
    }

    def 'should derive action and short names'() {
        expect:
        FlyteActionBuilder.actionName('3F5A9C0D1E2B4A6C8D0E2F4A6B8C0D1E') == 'nf-3f5a9c0d1e2b4a6c8d0e2f4a'
        FlyteActionBuilder.actionName('3f5a9c0d1e2b4a6c8d0e2f4a6b8c0d1e', 2) == 'nf-3f5a9c0d1e2b4a6c8d0e2f4a-2'
        FlyteActionBuilder.shortName('NFCORE_RNASEQ:RNASEQ:FASTQC') == 'nf.FASTQC'
        FlyteActionBuilder.shortName('foo (1)') == 'nf.foo__1_'
    }
}
