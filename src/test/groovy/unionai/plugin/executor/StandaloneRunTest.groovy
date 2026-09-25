package unionai.plugin.executor

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Timeout
import unionai.plugin.client.FlyteClient
import unionai.plugin.config.FlyteConfig

class StandaloneRunTest extends Specification {

    @TempDir
    Path workDir

    static FlyteConfig laptopConfig() {
        // what a user sets for `nextflow run` outside Flyte; no run, no pod env
        new FlyteConfig([endpoint: 'tenant.example.com', org: 'acme', project: 'proj', domain: 'development'], [FLYTE_API_KEY: 'k'])
    }

    def 'should create the run and point the executor at it'() {
        given:
        def config = laptopConfig()
        def client = Mock(FlyteClient)
        def run = new StandaloneRun(config, client, workDir)
        Map request

        when:
        run.start()

        then:
        1 * client.createRun(_) >> { args -> request = args[0]; [action: [id: [run: [org: 'acme', project: 'proj', domain: 'development', name: 'r42']]]] }
        1 * client.getActionDataUris([run: [org: 'acme', project: 'proj', domain: 'development', name: 'r42'], name: 'a0']) >>
            [inputsUri: 's3://bucket/metadata/v2/acme/proj/development/r42/a0/inputs.pb']

        and: 'task actions go into the new run, under its root'
        config.runName == 'r42'
        config.parentAction == 'a0'
        config.runOutputBase == 's3://bucket/metadata/v2/acme/proj/development/r42'
        run.url() == 'https://tenant.example.com/v2/domain/development/project/proj/runs/r42'

        and: 'the root is a waiter on the head, with a v2 runtime version'
        request.projectId == [organization: 'acme', name: 'proj', domain: 'development']
        def tmpl = request.taskSpec.taskTemplate
        tmpl.type == 'container'
        tmpl.metadata.runtime == [type: 'OTHER', flavor: 'nextflow', version: '2.10.0']
        tmpl.metadata.retries == [retries: 0]
        tmpl.container.image == FlyteConfig.DEFAULT_ROOT_IMAGE
        tmpl.container.command[0..2] == ['python', '-c', StandaloneRun.WATCHER]
        tmpl.container.command[3] == run.statusFile.toUriString()
        tmpl.container.command[4] == run.heartbeatFile.toUriString()
        tmpl.container.command[5] == '300'

        and: 'a heartbeat is written before the root starts watching'
        Files.exists(run.heartbeatFile)
        !Files.exists(run.statusFile)

        cleanup:
        run.finish(true)
    }

    def 'should report how the pipeline ended'() {
        given:
        def client = Mock(FlyteClient) {
            createRun(_) >> [action: [id: [run: [name: 'r1']]]]
            getActionDataUris(_) >> [inputsUri: 's3://b/r1/a0/inputs.pb']
        }
        def run = new StandaloneRun(laptopConfig(), client, workDir)
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
        def client = Mock(FlyteClient) {
            createRun(_) >> [action: [id: [run: [org: 'acme', project: 'proj', domain: 'development', name: 'r1']]]]
            getActionDataUris(_) >> [inputsUri: 's3://b/r1/a0/inputs.pb']
        }
        def run = new StandaloneRun(laptopConfig(), client, workDir)
        run.start()

        when:
        run.abort('Cancelled from Nextflow (SIGINT)')

        then:
        1 * client.abortRun([org: 'acme', project: 'proj', domain: 'development', name: 'r1'], 'Cancelled from Nextflow (SIGINT)')
        run.statusFile.text == 'aborted'
    }

    def 'should still end the run when aborting it fails'() {
        given:
        def client = Mock(FlyteClient) {
            createRun(_) >> [action: [id: [run: [name: 'r1']]]]
            getActionDataUris(_) >> [inputsUri: 's3://b/r1/a0/inputs.pb']
            abortRun(_, _) >> { throw new IOException('offline') }
        }
        def run = new StandaloneRun(laptopConfig(), client, workDir)
        run.start()

        when:
        run.abort('Cancelled from Nextflow (SIGTERM)')

        then: 'the root reads the status and fails the run, without waiting for the heartbeat timeout'
        noExceptionThrown()
        run.statusFile.text == 'aborted'
    }

    def 'should derive the run storage prefix from the root input location'() {
        expect:
        StandaloneRun.runOutputBase([inputsUri: 'gs://b/meta/r1/a0/inputs.pb']) == 'gs://b/meta/r1'

        when:
        StandaloneRun.runOutputBase([inputsUri: 's3://b/elsewhere/inputs.pb'])

        then:
        thrown(IllegalStateException)
    }

    // --- the root action's program, run with a local python against local files

    Process watch(Path status, Path heartbeat, int timeout) {
        new ProcessBuilder('python3', '-c', StandaloneRun.WATCHER, status.toString(), heartbeat.toString(), timeout.toString())
            .redirectErrorStream(true).start()
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
