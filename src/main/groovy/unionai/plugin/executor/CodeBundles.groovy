package unionai.plugin.executor

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPOutputStream

import com.google.protobuf.ByteString
import flyteidl2.dataproxy.DataproxyService.CreateUploadLocationRequest
import flyteidl2.dataproxy.DataproxyService.CreateUploadLocationResponse
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.processor.TaskRun
import unionai.flyte.controller.RunClient
import unionai.plugin.config.FlyteConfig

/**
 * Task scripts as code bundles, which Flyte's UI shows in an action's Code tab: `.command.sh`, as
 * Nextflow rendered it, in a `.tar.gz`, uploaded where the Flyte SDK uploads code bundles. The UI
 * downloads it from the browser, which a work dir bucket generally doesn't allow (no CORS rule).
 *
 * Uploads run on their own threads, so they never hold up task submission, and only cost the
 * code view when they fail: after a failure they pause for {@link #PAUSE}, during which tasks
 * get no code view. A script is uploaded once per run (by MD5), even by concurrent tasks.
 */
@Slf4j
@CompileStatic
class CodeBundles {

    static final String NAME = 'code.tar.gz'
    static final Duration PAUSE = Duration.ofMinutes(5)

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build()

    private final RunClient runs
    private final FlyteConfig config
    private final ExecutorService pool
    /** Upload of each bundle, by MD5: done, or in flight */
    private final Map<String,CompletableFuture<String>> uploads = new ConcurrentHashMap<>()
    private final AtomicBoolean warned = new AtomicBoolean()
    @PackageScope volatile long pausedUntil

    CodeBundles(RunClient runs, FlyteConfig config, int threads = 4) {
        this.runs = runs
        this.config = config
        this.pool = Executors.newFixedThreadPool(threads) { Runnable r ->
            final t = new Thread(r, 'flyte-code-bundles')
            t.daemon = true
            return t
        }
    }

    /**
     * @return the URI of the uploaded bundle of the script; null when it can't be uploaded. The
     *      future never completes exceptionally.
     */
    CompletableFuture<String> upload(Path script) {
        if( paused() )
            return CompletableFuture.completedFuture(null)
        return CompletableFuture.supplyAsync({ bundle(TaskRun.CMD_SCRIPT, Files.readAllBytes(script)) }, pool)
            .thenCompose { byte[] bundle -> uploadOnce(bundle) }
            .exceptionally { Throwable e -> failed(e) }
    }

    void close() {
        pool.shutdownNow()
    }

    private CompletableFuture<String> uploadOnce(byte[] bundle) {
        final md5 = MessageDigest.getInstance('MD5').digest(bundle)
        final key = md5.encodeHex().toString()
        final result = uploads.computeIfAbsent(key) { String k ->
            CompletableFuture.supplyAsync({ paused() ? null : store(bundle, md5) }, pool)
        }
        // a failed or skipped upload is tried again by the next task with the script
        result.whenComplete { String uri, Throwable e ->
            if( uri == null )
                uploads.remove(key, result)
        }
        return result
    }

    private String failed(Throwable e) {
        final cause = e.cause ?: e
        pausedUntil = System.currentTimeMillis() + PAUSE.toMillis()
        // once, not for every task: the tasks run anyway, without code view
        if( warned.compareAndSet(false, true) )
            log.warn "Unable to upload task scripts for Flyte's UI, actions will show no code -- ${cause.message}"
        log.debug "[FLYTE] Unable to upload a code bundle, pausing uploads for $PAUSE | ${cause.message}"
        return null
    }

    private boolean paused() {
        return System.currentTimeMillis() < pausedUntil
    }

    private String store(byte[] bundle, byte[] md5) {
        final location = runs.createUploadLocation(CreateUploadLocationRequest.newBuilder()
            .setOrg(config.org)
            .setProject(config.project)
            .setDomain(config.domain)
            .setFilename(NAME)
            .setContentMd5(ByteString.copyFrom(md5))
            .setAddContentMd5Metadata(true)
            .setContentLength(bundle.length)
            .build())
        put(location, bundle, md5)
        return location.nativeUrl
    }

    /**
     * PUT the content to the signed URL, with the headers the Flyte SDK sends
     */
    static void put(CreateUploadLocationResponse location, byte[] content, byte[] md5) {
        final request = HttpRequest.newBuilder(URI.create(location.signedUrl))
            .timeout(Duration.ofSeconds(60))
            .header('Content-MD5', Base64.encoder.encodeToString(md5))
            .PUT(HttpRequest.BodyPublishers.ofByteArray(content))
        if( location.nativeUrl.startsWith('abfs') )
            request.header('x-ms-blob-type', 'BlockBlob')
        // the HTTP client sets Content-Length itself, from the body, and rejects it as a header
        location.headersMap.each { String name, String value ->
            if( !name.equalsIgnoreCase('Content-Length') )
                request.setHeader(name, value)
        }
        final response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString())
        if( response.statusCode().intdiv(100) != 2 )
            throw new IOException("Upload to ${location.nativeUrl} failed with HTTP ${response.statusCode()}: ${response.body()?.take(500)}")
    }

    /**
     * @return a `.tar.gz` holding one file, readable by any tar reader (POSIX ustar)
     */
    static byte[] bundle(String name, byte[] content) {
        final header = new byte[512]
        final put = { int offset, int length, String value ->
            final bytes = value.getBytes(StandardCharsets.US_ASCII)
            System.arraycopy(bytes, 0, header, offset, Math.min(bytes.length, length))
        }
        put(0, 100, name)
        put(100, 8, '0000644')
        put(108, 8, '0000000')
        put(116, 8, '0000000')
        put(124, 12, String.format('%011o', content.length))
        // no timestamp: the same script makes the same bundle, uploaded once
        put(136, 12, '00000000000')
        put(148, 8, '        ')
        put(156, 1, '0')
        put(257, 6, 'ustar')
        put(263, 2, '00')
        long sum = 0
        for( byte b : header )
            sum += (b & 0xff)
        put(148, 8, String.format('%06o', sum) + '\u0000 ')

        final bytes = new ByteArrayOutputStream()
        new GZIPOutputStream(bytes).withCloseable { GZIPOutputStream gz ->
            gz.write(header)
            gz.write(content)
            gz.write(new byte[(512 - content.length % 512) % 512])
            // end of archive: two empty blocks
            gz.write(new byte[1024])
        }
        return bytes.toByteArray()
    }
}
