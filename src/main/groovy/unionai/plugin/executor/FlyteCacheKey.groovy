package unionai.plugin.executor

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.extension.FilesEx

/**
 * Flyte cache key for a Nextflow task, stable across Nextflow sessions.
 *
 * Nextflow's own task hash starts with the session id, so it changes with every run that
 * doesn't resume the same session, and a Flyte cache keyed on it would never hit across runs.
 * This key covers what Nextflow's hash covers except the session: the process, the rendered
 * script (with its `val` inputs and globals), the container, the environment, the pipeline's
 * `bin` scripts, and the task's input files, identified by where they come from:
 * <ul>
 * <li>a file produced by another nf-flyte task: that task's cache key and the file's path in it
 *     (saved as `.flyte/cache_key` in every task dir), so keys chain through the pipeline without
 *     reading any file content;</li>
 * <li>a foreign file Nextflow staged into the work dir (http, another cloud, a local path): its
 *     name and a hash of its content, computed once per run, since the staged copy's location
 *     depends on the session;</li>
 * <li>any other file: its location, size and modification time, like Nextflow's standard hashing.</li>
 * </ul>
 */
@Slf4j
@CompileStatic
class FlyteCacheKey {

    static final String KEY_FILE = '.flyte/cache_key'

    private static final Pattern TASK_DIR = ~/^[0-9a-f]{2}\/[0-9a-f]{30}\/(.+)$/
    private static final Pattern STAGE_DIR = ~/^stage-[0-9a-f-]{36}\/(.+)$/

    private final Path workDir
    private final Map<String,String> producerKeys = new ConcurrentHashMap<>()
    private final Map<String,String> contentHashes = new ConcurrentHashMap<>()

    FlyteCacheKey(Path workDir) {
        this.workDir = workDir
    }

    /**
     * @param inputs the task's input files, by stage name
     * @param identity everything else that determines the task's result
     */
    String compute(Map<String,Path> inputs, List<String> identity) {
        final digest = MessageDigest.getInstance('SHA-256')
        for( String it : identity )
            update(digest, it)
        for( String name : new TreeSet<String>(inputs.keySet()) ) {
            update(digest, name)
            update(digest, fingerprint(inputs.get(name)))
        }
        return digest.digest().encodeHex().toString()
    }

    /** Record a submitted task's key in its task dir, for the tasks that consume its outputs */
    void save(Path taskDir, String key) {
        final file = taskDir.resolve(KEY_FILE)
        Files.createDirectories(file.parent)
        Files.write(file, key.getBytes(StandardCharsets.UTF_8))
        producerKeys.put(FilesEx.toUriString(taskDir), key)
    }

    protected String fingerprint(Path input) {
        final rel = relativeToWorkDir(input)
        if( rel ) {
            final task = TASK_DIR.matcher(rel)
            if( task.matches() ) {
                final producer = producerKey(workDir.resolve(rel.substring(0, 33)))
                if( producer )
                    return "task:$producer:${task.group(1)}"
            }
            if( STAGE_DIR.matcher(rel).matches() )
                return "staged:${input.fileName}:${contentHash(input)}"
        }
        return "file:${FilesEx.toUriString(input)}:${size(input)}:${modified(input)}"
    }

    private String relativeToWorkDir(Path input) {
        final base = FilesEx.toUriString(workDir).replaceFirst(/\/+$/, '') + '/'
        final uri = FilesEx.toUriString(input)
        return uri.startsWith(base) ? uri.substring(base.length()) : null
    }

    private String producerKey(Path taskDir) {
        final id = FilesEx.toUriString(taskDir)
        final known = producerKeys.get(id)
        if( known )
            return known
        try {
            final key = new String(Files.readAllBytes(taskDir.resolve(KEY_FILE)), StandardCharsets.UTF_8).trim()
            producerKeys.put(id, key)
            return key
        }
        catch( NoSuchFileException e ) {
            // produced before nf-flyte recorded keys, or by another executor
            return null
        }
    }

    private String contentHash(Path p) {
        return contentHashes.computeIfAbsent(FilesEx.toUriString(p)) { String uri -> contentDigest(p) }
    }

    private static String contentDigest(Path p) {
        final digest = MessageDigest.getInstance('SHA-256')
        if( Files.isDirectory(p) ) {
            final files = new ArrayList<Path>()
            Files.walk(p).withCloseable { stream -> stream.filter { Path f -> Files.isRegularFile(f) }.forEach { Path f -> files.add(f) } }
            files.sort { Path f -> p.relativize(f).toString() }
            for( Path f : files ) {
                update(digest, p.relativize(f).toString())
                update(digest, contentDigest(f))
            }
        }
        else {
            final buf = new byte[1 << 20]
            Files.newInputStream(p).withCloseable { InputStream in ->
                int n
                while( (n = in.read(buf)) > 0 )
                    digest.update(buf, 0, n)
            }
        }
        return digest.digest().encodeHex().toString()
    }

    private static String size(Path p) {
        try {
            return Files.isDirectory(p) ? 'dir' : Files.size(p).toString()
        }
        catch( IOException e ) {
            log.debug "[FLYTE] cannot read size of ${FilesEx.toUriString(p)} | ${e.message}"
            return '?'
        }
    }

    private static String modified(Path p) {
        try {
            return Files.getLastModifiedTime(p).toMillis().toString()
        }
        catch( IOException e ) {
            return '?'
        }
    }

    private static void update(MessageDigest digest, String value) {
        final bytes = (value ?: '').getBytes(StandardCharsets.UTF_8)
        // length prefix, so ["ab","c"] and ["a","bc"] differ
        digest.update(bytes.length.toString().getBytes(StandardCharsets.UTF_8))
        digest.update((byte) 0)
        digest.update(bytes)
    }

    /** Hash of the pipeline's local `bin` scripts, which tasks run through their PATH */
    static String binHash(Path binDir) {
        if( !binDir || !Files.isDirectory(binDir) )
            return ''
        final digest = MessageDigest.getInstance('SHA-256')
        final files = new ArrayList<Path>()
        Files.walk(binDir).withCloseable { stream -> stream.filter { Path p -> Files.isRegularFile(p) }.forEach { Path p -> files.add(p) } }
        files.sort { Path p -> binDir.relativize(p).toString() }
        for( Path p : files ) {
            update(digest, binDir.relativize(p).toString())
            digest.update(Files.readAllBytes(p))
        }
        return digest.digest().encodeHex().toString()
    }
}
