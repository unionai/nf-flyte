package unionai.plugin.executor

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.common.Identifier.RunIdentifier
import flyteidl2.core.Tasks.RuntimeMetadata
import flyteidl2.common.Run.OffloadedInputData
import flyteidl2.dataproxy.DataproxyService.UploadInputsRequest
import flyteidl2.workflow.RunDefinition.Action
import flyteidl2.workflow.RunDefinition.Run
import flyteidl2.workflow.RunServiceOuterClass.CreateRunRequest
import flyteidl2.workflow.RunServiceOuterClass.GetActionDataURIsResponse
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Timeout
import unionai.flyte.controller.RunClient
import unionai.plugin.config.FlyteConfig

class StandaloneRunTest extends Specification {

    @TempDir
    Path workDir

    static FlyteConfig laptopConfig() {
        // what a user sets for `nextflow run` outside Flyte; no run, no pod env
        new FlyteConfig([endpoint: 'tenant.example.com', org: 'acme', project: 'proj', domain: 'development'], [FLYTE_API_KEY: 'k'])
    }

    static RunIdentifier runId(String name) {
        RunIdentifier.newBuilder().setOrg('acme').setProject('proj').setDomain('development').setName(name).build()
    }

    static Run created(String name) {
        Run.newBuilder().setAction(Action.newBuilder().setId(ActionIdentifier.newBuilder().setRun(runId(name)).setName('a0'))).build()
    }

    static GetActionDataURIsResponse uris(String inputsUri) {
        GetActionDataURIsResponse.newBuilder().setInputsUri(inputsUri).build()
    }

    def 'should create the run and point the executor at it'() {
        given:
        def config = laptopConfig()
        def runs = Mock(RunClient)
        def run = new StandaloneRun(config, runs, workDir, 'nf-core/rnaseq')
        CreateRunRequest request
        UploadInputsRequest upload

        when:
        run.start()

        then: 'empty inputs are stored first, as the Flyte SDK does'
        1 * runs.uploadInputs(_) >> { args -> upload = args[0]; OffloadedInputData.newBuilder().setUri('s3://b/offloaded/inputs.pb').setInputsHash('h').build() }
        1 * runs.createRun(_) >> { args -> request = args[0]; created('r42') }
        1 * runs.getActionDataUris(ActionIdentifier.newBuilder().setRun(runId('r42')).setName('a0').build()) >>
            uris('s3://bucket/metadata/v2/acme/proj/development/r42/a0/inputs.pb')

        and: 'task actions go into the new run, under its root'
        config.runName == 'r42'
        config.parentAction == 'a0'
        config.runOutputBase == 's3://bucket/metadata/v2/acme/proj/development/r42'
        run.url() == 'https://tenant.example.com/v2/domain/development/project/proj/runs/r42'

        and: 'the root is a waiter on the head, with a v2 runtime version'
        request.runId.project == 'proj'
        request.runId.domain == 'development'
        def tmpl = request.taskSpec.taskTemplate
        tmpl.type == 'container'

        and: 'the root is named after the pipeline'
        request.taskSpec.shortName == 'nf-core/rnaseq'
        tmpl.id.name == 'nf.nf-core_rnaseq'
        tmpl.metadata.runtime.type == RuntimeMetadata.RuntimeType.OTHER
        tmpl.metadata.runtime.flavor == 'nextflow'
        tmpl.metadata.runtime.version == '2.10.0'
        tmpl.metadata.retries.retries == 0
        tmpl.container.image == FlyteConfig.DEFAULT_ROOT_IMAGE
        tmpl.container.commandList[0..2] == ['python', '-c', StandaloneRun.WATCHER]
        tmpl.container.commandList[3] == run.statusFile.toUriString()
        tmpl.container.commandList[4] == run.heartbeatFile.toUriString()
        tmpl.container.commandList[5] == '300'
        tmpl.container.commandList[6] == '{{.outputPrefix}}'

        and: 'the run is named up front and references the inputs stored under it'
        upload.inputs.literalsCount == 0
        upload.taskSpec == request.taskSpec
        upload.runId == request.runId
        request.runId.name ==~ /nf[a-z0-9]{18}/
        request.runId.org == 'acme'
        request.offloadedInputData.uri == 's3://b/offloaded/inputs.pb'

        and: 'a heartbeat is written before the root starts watching'
        Files.exists(run.heartbeatFile)
        !Files.exists(run.statusFile)

        cleanup:
        run.finish(true)
    }

    def 'should name the root nextflow without a pipeline name'() {
        given:
        def runs = Mock(RunClient)
        def run = new StandaloneRun(laptopConfig(), runs, workDir)

        expect:
        run.rootSpec().shortName == 'nextflow'
        run.rootSpec().taskTemplate.id.name == 'nf.nextflow'
    }

    def 'should use its own run name when a retried create finds the run'() {
        given:
        def config = laptopConfig()
        def runs = Mock(RunClient) {
            uploadInputs(_) >> OffloadedInputData.newBuilder().setUri('s3://b/offloaded/inputs.pb').setInputsHash('h').build()
            createRun(_) >> null
            getActionDataUris(_) >> { args -> uris("s3://b/${args[0].run.name}/a0/inputs.pb") }
        }
        def run = new StandaloneRun(config, runs, workDir)

        when:
        run.start()

        then:
        config.runName ==~ /nf[a-z0-9]{18}/
        config.runOutputBase == "s3://b/${config.runName}"

        cleanup:
        run.finish(true)
    }

    def 'should report how the pipeline ended'() {
        given:
        def runs = Mock(RunClient) {
            uploadInputs(_) >> OffloadedInputData.newBuilder().setUri('s3://b/offloaded/inputs.pb').setInputsHash('h').build()
            createRun(_) >> created('r1')
            getActionDataUris(_) >> uris('s3://b/r1/a0/inputs.pb')
        }
        def run = new StandaloneRun(laptopConfig(), runs, workDir)
        run.start()

        when:
        run.finish(success)

        then:
        run.statusFile.text == expected

        where:
        success | expected
        true    | 'succeeded'
        false   | 'failed'
    }

    def 'should abort the run when the user stops Nextflow'() {
        given:
        def runs = Mock(RunClient) {
            uploadInputs(_) >> OffloadedInputData.newBuilder().setUri('s3://b/offloaded/inputs.pb').setInputsHash('h').build()
            createRun(_) >> created('r1')
            getActionDataUris(_) >> uris('s3://b/r1/a0/inputs.pb')
        }
        def run = new StandaloneRun(laptopConfig(), runs, workDir)
        run.start()

        when:
        run.abort('Cancelled from Nextflow (SIGINT)')

        then:
        1 * runs.abortRun(runId('r1'), 'Cancelled from Nextflow (SIGINT)')
        run.statusFile.text == 'aborted'
    }

    def 'should still end the run when aborting it fails'() {
        given:
        def runs = Mock(RunClient) {
            uploadInputs(_) >> OffloadedInputData.newBuilder().setUri('s3://b/offloaded/inputs.pb').setInputsHash('h').build()
            createRun(_) >> created('r1')
            getActionDataUris(_) >> uris('s3://b/r1/a0/inputs.pb')
            abortRun(_, _) >> { throw new IOException('offline') }
        }
        def run = new StandaloneRun(laptopConfig(), runs, workDir)
        run.start()

        when:
        run.abort('Cancelled from Nextflow (SIGTERM)')

        then: 'the root reads the status and fails the run, without waiting for the heartbeat timeout'
        noExceptionThrown()
        run.statusFile.text == 'aborted'
    }

    def 'should still end the run when aborting it fails with an error'() {
        given:
        def runs = Mock(RunClient) {
            uploadInputs(_) >> OffloadedInputData.newBuilder().setUri('s3://b/offloaded/inputs.pb').setInputsHash('h').build()
            createRun(_) >> created('r1')
            getActionDataUris(_) >> uris('s3://b/r1/a0/inputs.pb')
            abortRun(_, _) >> { throw new NoClassDefFoundError('unionai/flyte/controller/internal/Missing') }
        }
        def run = new StandaloneRun(laptopConfig(), runs, workDir)
        run.start()

        when:
        run.abort('Cancelled from Nextflow (SIGINT)')

        then:
        thrown(NoClassDefFoundError)
        run.statusFile.text == 'aborted'
    }

    def 'should derive the run storage prefix from the root input location'() {
        expect:
        StandaloneRun.runOutputBase(uris('gs://b/meta/r1/a0/inputs.pb')) == 'gs://b/meta/r1'

        when:
        StandaloneRun.runOutputBase(uris('s3://b/elsewhere/inputs.pb'))

        then:
        thrown(IllegalStateException)
    }

    // --- the root action's program, run with a local python against local files

    Process watch(Path status, Path heartbeat, int timeout, Path outputs = null) {
        def cmd = ['python3', '-c', StandaloneRun.WATCHER, status.toString(), heartbeat.toString(), timeout.toString()]
        if( outputs )
            cmd << outputs.toString()
        new ProcessBuilder(cmd).redirectErrorStream(true).start()
    }

    def 'root should write empty outputs when the pipeline succeeds'() {
        given:
        def status = workDir.resolve('status')
        status.text = result
        def prefix = workDir.resolve('out/a0/0')

        when:
        def p = watch(status, workDir.resolve('heartbeat'), 300, prefix)
        p.inputStream.text

        then:
        p.waitFor(20, TimeUnit.SECONDS)
        Files.exists(prefix.resolve('outputs.pb')) == written
        !written || prefix.resolve('outputs.pb').size() == 0

        where:
        result      | written
        'succeeded' | true
        'failed'    | false
    }

    def 'root should end with the pipeline result'() {
        given:
        def status = workDir.resolve('status')
        status.text = result

        when:
        def p = watch(status, workDir.resolve('heartbeat'), 300)
        def out = p.inputStream.text

        then:
        p.waitFor(20, TimeUnit.SECONDS)
        p.exitValue() == code
        out.contains("Nextflow head reported: $result")

        where:
        result      | code
        'succeeded' | 0
        'failed'    | 1
        'aborted'   | 1
    }

    @Timeout(40)
    def 'root should fail when the head stops sending heartbeats'() {
        given: 'a heartbeat that is already an hour old'
        def heartbeat = workDir.resolve('heartbeat')
        heartbeat.text = ((System.currentTimeMillis() / 1000) - 3600).toString()

        when:
        def p = watch(workDir.resolve('status'), heartbeat, 1)
        def out = p.inputStream.text

        then:
        p.waitFor(30, TimeUnit.SECONDS)
        p.exitValue() == 1
        out.contains('No heartbeat from the Nextflow head')
    }
}
