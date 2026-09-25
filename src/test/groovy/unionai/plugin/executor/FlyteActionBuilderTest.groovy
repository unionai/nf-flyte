package unionai.plugin.executor

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
        action.actionId == [run: [org: 'acme', project: 'proj', domain: 'development', name: 'r123'], name: 'nf-abc']
        action.parentActionName == 'a0'
        action.inputUri == 's3://bucket/work/ab/cdef/.command.inputs.pb'
        action.runOutputBase == 's3://bucket/base'
        action.group == 'NFCORE_RNASEQ:RNASEQ:FASTQC'
        !action.task.containsKey('cacheKey')

        and:
        def tmpl = action.task.spec.taskTemplate
        action.task.spec.shortName == 'nf.FASTQC'
        tmpl.type == 'raw-container'
        tmpl.id == [resourceType: 'TASK', org: 'acme', project: 'proj', domain: 'development', name: 'nf.FASTQC', version: 'nextflow']
        tmpl.metadata == [runtime: [type: 'OTHER', flavor: 'nextflow'], retries: [retries: 0], timeout: '3600s']

        and: 'every staged input is a File or Dir input, the task dir comes back as a Dir'
        tmpl.interface.inputs.variables == [
            [key: 'in_0', value: [type: [blob: [format: '', dimensionality: 'SINGLE']]]],
            [key: 'in_1', value: [type: [blob: [format: '', dimensionality: 'MULTIPART']]]],
            [key: 'nf_cmd_run', value: [type: [blob: [format: '', dimensionality: 'SINGLE']]]],
            [key: 'nf_cmd_sh', value: [type: [blob: [format: '', dimensionality: 'SINGLE']]]],
            [key: 'nf_bin', value: [type: [blob: [format: '', dimensionality: 'MULTIPART']]]],
        ]
        tmpl.interface.outputs.variables == [[key: 'task_dir', value: [type: [blob: [format: '', dimensionality: 'MULTIPART']]]]]

        and: 'copilot moves the data, keeping file names'
        def c = tmpl.container
        c.image == 'quay.io/biocontainers/fastqc:0.12.1'
        c.command == ['bash', '-c', FlyteTaskLauncher.ENTRYPOINT]
        c.dataConfig == [
            enabled: true,
            inputPath: '/var/inputs',
            outputPath: '/var/outputs',
            format: 'JSON',
            ioStrategy: [downloadMode: 'DOWNLOAD_EAGER', uploadMode: 'UPLOAD_ON_EXIT'],
            fileInputLayout: 'NAMED_DIR',
        ]
        c.env == [[key: 'FOO', value: 'bar']]
        c.resources.requests == [[name: 'CPU', value: '2'], [name: 'MEMORY', value: '4096Mi']]
        c.resources.limits == c.resources.requests
    }

    def 'should cache under the given key'() {
        given:
        def b = builder()
        b.cacheKey = 'a1b2c3'

        when:
        def action = b.build()

        then:
        action.task.cacheKey == 'a1b2c3'
        action.task.spec.taskTemplate.metadata.discoverable == true
        action.task.spec.taskTemplate.metadata.discoveryVersion == 'nf-flyte-1'
    }

    def 'should set the queue and omit empty settings'() {
        when:
        def action = builder(queue: 'gpu').build()

        then:
        action.task.queue == 'gpu'
        !action.containsKey('group')
        !action.task.spec.taskTemplate.container.containsKey('resources')
        !action.task.spec.taskTemplate.container.containsKey('env')
        !action.task.spec.taskTemplate.metadata.containsKey('timeout')
        !action.task.spec.taskTemplate.metadata.containsKey('discoverable')
    }

    def 'should derive action and short names'() {
        expect:
        FlyteActionBuilder.actionName('3F5A9C0D1E2B4A6C8D0E2F4A6B8C0D1E') == 'nf-3f5a9c0d1e2b4a6c8d0e2f4a'
        FlyteActionBuilder.shortName('NFCORE_RNASEQ:RNASEQ:FASTQC') == 'nf.FASTQC'
        FlyteActionBuilder.shortName('foo (1)') == 'nf.foo__1_'
    }
}
