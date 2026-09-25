package unionai.plugin.executor

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

class FlyteCacheKeyTest extends Specification {

    @TempDir
    Path tmp

    static final String TASK_HASH = '0123456789abcdef0123456789abcd'  // 30 hex chars, like Nextflow's
    static final List<String> IDENTITY = ['nf-flyte-1', 'UPPER', 'tr a-z A-Z < in.txt', 'ubuntu:24.04', '{}', '', '']

    Path file(Path p, String text) {
        Files.createDirectories(p.parent)
        p.text = text
        return p
    }

    def 'should not depend on the Nextflow session or the work dir'() {
        given: 'the same pipeline run twice, in two different work dirs'
        def keys = ['run1', 'run2'].collect { run ->
            def wd = Files.createDirectories(tmp.resolve(run))
            def cache = new FlyteCacheKey(wd)
            // an upstream task that produced chunk_1.txt, submitted with key K
            def upstream = wd.resolve("ab/$TASK_HASH")
            file(upstream.resolve('chunk_1.txt'), 'sample 1')
            cache.save(upstream, 'K')
            cache.compute(['in.txt': upstream.resolve('chunk_1.txt')], IDENTITY)
        }

        expect:
        keys[0] == keys[1]
    }

    def 'should change when an upstream task or the task itself changes'() {
        given:
        def wd = Files.createDirectories(tmp.resolve('wd'))
        def cache = new FlyteCacheKey(wd)
        def upstream = wd.resolve("ab/$TASK_HASH")
        def input = file(upstream.resolve('chunk_1.txt'), 'sample 1')
        cache.save(upstream, 'K1')
        def base = cache.compute(['in.txt': input], IDENTITY)

        expect: 'a different script'
        cache.compute(['in.txt': input], IDENTITY.collect { it == 'tr a-z A-Z < in.txt' ? 'tr A-Z a-z < in.txt' : it }) != base

        and: 'a different stage name'
        cache.compute(['other.txt': input], IDENTITY) != base

        and: 'a different upstream key'
        new FlyteCacheKey(wd).with { it.save(upstream, 'K2'); it.compute(['in.txt': input], IDENTITY) } != base
    }

    def 'should read producer keys saved by an earlier head'() {
        given: 'a key saved by one head (e.g. before a retry), read by a fresh one'
        def wd = Files.createDirectories(tmp.resolve('wd'))
        def upstream = wd.resolve("cd/$TASK_HASH")
        def input = file(upstream.resolve('out.txt'), 'x')
        new FlyteCacheKey(wd).save(upstream, 'K')

        expect:
        new FlyteCacheKey(wd).fingerprint(input) == 'task:K:out.txt'
        upstream.resolve('.flyte/cache_key').text == 'K'
    }

    def 'should identify staged foreign files by content, not by where the session staged them'() {
        given: 'the same download staged by two sessions: Nextflow picks a session-dependent path'
        def a = file(tmp.resolve('wd1/stage-11111111-2222-3333-4444-555555555555/9f/abc/samplesheet.csv'), 'a,b')
        def b = file(tmp.resolve('wd2/stage-66666666-7777-8888-9999-000000000000/e1/xyz/samplesheet.csv'), 'a,b')
        def other = file(tmp.resolve('wd2/stage-66666666-7777-8888-9999-000000000000/77/qqq/samplesheet.csv'), 'a,c')

        expect:
        new FlyteCacheKey(tmp.resolve('wd1')).fingerprint(a) == new FlyteCacheKey(tmp.resolve('wd2')).fingerprint(b)
        new FlyteCacheKey(tmp.resolve('wd1')).fingerprint(a).startsWith('staged:samplesheet.csv:')

        and: 'same name and size, different content: never the same key'
        new FlyteCacheKey(tmp.resolve('wd2')).fingerprint(other) != new FlyteCacheKey(tmp.resolve('wd2')).fingerprint(b)
    }

    def 'should fall back to location, size and time'() {
        given: 'an external input, and a task dir without a saved key'
        def wd = Files.createDirectories(tmp.resolve('wd'))
        def external = file(tmp.resolve('data/reads.fq'), 'ACGT')
        def unkeyed = file(wd.resolve("ef/$TASK_HASH/out.txt"), 'x')
        def cache = new FlyteCacheKey(wd)

        expect:
        cache.fingerprint(external) ==~ /file:.*\/data\/reads\.fq:4:\d+/
        cache.fingerprint(unkeyed).startsWith('file:')
    }

    def 'should hash bin scripts by name and content'() {
        given:
        def bin = Files.createDirectories(tmp.resolve('bin'))
        file(bin.resolve('tool.sh'), 'echo 1')
        def h1 = FlyteCacheKey.binHash(bin)

        expect:
        FlyteCacheKey.binHash(bin) == h1
        FlyteCacheKey.binHash(null) == ''

        when:
        bin.resolve('tool.sh').text = 'echo 2'

        then:
        FlyteCacheKey.binHash(bin) != h1
    }
}
