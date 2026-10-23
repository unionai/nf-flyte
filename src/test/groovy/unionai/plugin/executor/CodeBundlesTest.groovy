package unionai.plugin.executor

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import flyteidl2.dataproxy.DataproxyService.CreateUploadLocationRequest
import flyteidl2.dataproxy.DataproxyService.CreateUploadLocationResponse
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Timeout
import unionai.flyte.controller.RunClient

@Timeout(30)
class CodeBundlesTest extends Specification {

    @TempDir
    Path workDir

    RunClient runs = Mock(RunClient)
    CodeBundles bundles
    HttpServer server
    List<Map> received = Collections.synchronizedList([])

    def setup() {
        bundles = new CodeBundles(runs, FlyteActionBuilderTest.config())
        server = HttpServer.create(new InetSocketAddress('127.0.0.1', 0), 0)
        server.createContext('/upload') { HttpExchange ex ->
            received << [
                method: ex.requestMethod,
                md5: ex.requestHeaders.get('Content-MD5'),
                extra: ex.requestHeaders.getFirst('X-Amz-Meta-Test'),
                body: ex.requestBody.bytes,
            ]
            ex.sendResponseHeaders(200, -1)
            ex.close()
        }
        server.start()
    }

    def cleanup() {
        bundles.close()
        server.stop(0)
    }

    /** What dataproxy returns, with its headers: the HTTP client must not set these twice, or at all */
    CreateUploadLocationResponse location(CreateUploadLocationRequest request) {
        CreateUploadLocationResponse.newBuilder()
            .setSignedUrl("http://127.0.0.1:${server.address.port}/upload")
            .setNativeUrl("s3://fast/${request.contentMd5.toByteArray().encodeHex()}/code.tar.gz")
            .putHeaders('X-Amz-Meta-Test', 'yes')
            .putHeaders('Content-Length', '24')
            .putHeaders('Content-MD5', Base64.encoder.encodeToString(request.contentMd5.toByteArray()))
            .build()
    }

    Path script(String text, String dir = 'ab/cdef') {
        final path = Files.createDirectories(workDir.resolve(dir)).resolve('.command.sh')
        path.text = text
        return path
    }

    def 'should pack the task script as a tar.gz any tar reader extracts'() {
        given:
        def script = '#!/bin/bash -ue\nfastqc --threads 2 sample.fq.gz\n'.bytes
        def bundle = workDir.resolve('code.tar.gz')
        bundle.bytes = CodeBundles.bundle('.command.sh', script)
        def out = Files.createDirectories(workDir.resolve('out'))

        when:
        def tar = new ProcessBuilder('tar', '-xzf', bundle.toString(), '-C', out.toString()).redirectErrorStream(true).start()
        def log = tar.inputStream.text

        then:
        tar.waitFor() == 0
        log == ''
        out.toFile().list() as List == ['.command.sh']
        out.resolve('.command.sh').bytes == script
    }

    def 'should make the same bundle from the same script'() {
        expect:
        CodeBundles.bundle('.command.sh', 'echo hi'.bytes) == CodeBundles.bundle('.command.sh', 'echo hi'.bytes)
    }

    def 'should upload the bundle where dataproxy says, with the headers it asks for'() {
        given:
        def path = script('#!/bin/bash -ue\necho hello\n')
        CreateUploadLocationRequest request

        when:
        def uri = bundles.upload(path).get()

        then:
        1 * runs.createUploadLocation(_) >> { args -> request = args[0]; location(request) }
        uri == "s3://fast/${request.contentMd5.toByteArray().encodeHex()}/code.tar.gz"
        request.project == 'proj'
        request.domain == 'development'
        request.org == 'acme'
        request.filename == 'code.tar.gz'
        request.addContentMd5Metadata
        received.size() == 1
        def put = received[0]
        put.method == 'PUT'
        put.extra == 'yes'
        put.body == CodeBundles.bundle('.command.sh', path.bytes)
        request.contentMd5.toByteArray() == MessageDigest.getInstance('MD5').digest(put.body as byte[])
        put.md5 == [Base64.encoder.encodeToString(request.contentMd5.toByteArray())]
        request.contentLength == (put.body as byte[]).length
    }

    def 'should upload a script once, even for tasks uploading it at the same time'() {
        given: 'the first upload is held until all tasks have asked'
        def asked = new CountDownLatch(1)
        def paths = (1..5).collect { script('echo same\n', "t$it") }
        runs.createUploadLocation(_) >> { args -> asked.await(10, TimeUnit.SECONDS); location(args[0]) }

        when:
        def futures = paths.collect { bundles.upload(it) }
        sleep 200
        asked.countDown()
        def uris = futures.collect { it.get() }
        def again = bundles.upload(script('echo same\n', 't6')).get()

        then:
        received.size() == 1
        uris.unique().size() == 1
        uris[0] != null
        again == uris[0]
    }

    def 'should leave tasks without code view, and pause uploads, when an upload fails'() {
        when:
        def first = bundles.upload(script('echo one\n', 'a')).get()

        then:
        1 * runs.createUploadLocation(_) >> { throw new RuntimeException('PERMISSION_DENIED') }
        first == null

        when: 'the next task, during the pause'
        def second = bundles.upload(script('echo two\n', 'b')).get()

        then: 'no call to dataproxy'
        0 * runs.createUploadLocation(_)
        second == null

        when: 'after the pause, the failed script is tried again'
        bundles.pausedUntil = 0
        def third = bundles.upload(script('echo one\n', 'c')).get()

        then:
        1 * runs.createUploadLocation(_) >> { args -> location(args[0]) }
        third != null
    }

    def 'should leave a task without code view when its script is missing'() {
        when:
        def uri = bundles.upload(workDir.resolve('missing/.command.sh')).get()

        then:
        0 * runs.createUploadLocation(_)
        uri == null
    }
}
