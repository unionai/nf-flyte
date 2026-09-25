package unionai.plugin.executor

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

import nextflow.processor.TaskBean
import nextflow.processor.TaskRun
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Timeout
import unionai.plugin.executor.FlyteTaskHandler.TaskFailure
import unionai.plugin.executor.FlyteTaskLauncher.StagedInput

class FlyteTaskLauncherTest extends Specification {

    @TempDir
    Path tmp

    // a stand-in for Nextflow's .command.run: writes what the real wrapper would into the task dir
    static final String FAKE_WRAPPER = '''\
        W=$(dirname "$0")
        echo hello > "$W/.command.out"
        echo boom > "$W/.command.err"
        echo result > "$W/result.txt"
        command -v mytool > "$W/which.txt" || true
        if [ -n "${FAKE_WAIT:-}" ]; then trap 'echo got-term >> "$W/.command.err"; exit 143' TERM; touch "$W/started"; sleep 60 & wait; fi
        exit ${FAKE_EXIT:-0}
        '''.stripIndent()

    /** The pod entry point, with /var/inputs and /var/outputs moved under the temp dir */
    Process startEntrypoint(Map<String,String> env = [:]) {
        def inputs = tmp.resolve('inputs'), outputs = tmp.resolve('outputs')
        Files.createDirectories(inputs.resolve('nf_cmd_run'))
        Files.createDirectories(inputs.resolve('nf_cmd_sh'))
        Files.createDirectories(inputs.resolve('nf_bin'))
        Files.createDirectories(outputs)
        inputs.resolve('nf_cmd_run/.command.run').text = FAKE_WRAPPER
        inputs.resolve('nf_cmd_sh/.command.sh').text = 'echo task script'
        inputs.resolve('nf_bin/mytool').text = '#!/bin/sh\necho tool'  // not executable yet, like after a download
        def script = FlyteTaskLauncher.ENTRYPOINT
            .replace('/var/inputs', inputs.toString())
            .replace('/var/outputs', outputs.toString())
        def pb = new ProcessBuilder('bash', '-c', script).redirectErrorStream(true)
        pb.environment().putAll(env)
        return pb.start()
    }

    Path out(String name) { tmp.resolve('outputs').resolve(name) }

    def 'should run the wrapper and leave the task dir for copilot to upload'() {
        when:
        def p = startEntrypoint()
        p.inputStream.text

        then:
        p.waitFor() == 0
        !Files.exists(out('_ERROR'))
        out('task_dir/.command.run').text == FAKE_WRAPPER
        out('task_dir/.command.sh').text == 'echo task script'
        out('task_dir/result.txt').text == 'result\n'
        // pipeline bin/ scripts are made executable and put on the PATH
        out('task_dir/which.txt').text.trim() == tmp.resolve('inputs/nf_bin/mytool').toString()
    }

    def 'should report a failed task through the error file and exit 0'() {
        when:
        def p = startEntrypoint(FAKE_EXIT: '3')
        p.inputStream.text

        then: 'copilot turns the error file into a failed action; the container itself succeeds'
        p.waitFor() == 0
        def error = out('_ERROR').text
        error.startsWith('nf-flyte task failed: exit=3\n')

        and: 'the head gets back what Nextflow needs'
        def failure = TaskFailure.parse('User Error: ' + error)
        failure.exitCode == 3
        failure.stderr == 'boom\n'
        failure.stdout == 'hello\n'
    }

    @Timeout(30)
    def 'should forward SIGTERM to the wrapper'() {
        given:
        def p = startEntrypoint(FAKE_WAIT: '1')
        def started = out('task_dir/started')
        for( int i = 0; i < 100 && !Files.exists(started); i++ ) sleep 100

        when:
        new ProcessBuilder('kill', '-TERM', p.pid().toString()).start().waitFor()

        then:
        p.waitFor(20, TimeUnit.SECONDS)
        p.exitValue() == 0
        TaskFailure.parse(out('_ERROR').text).exitCode == 143
        out('_ERROR').text.contains('got-term')
    }

    def 'should parse only the entry point failure format'() {
        expect:
        TaskFailure.parse(null) == null
        TaskFailure.parse('Pod failed. [x] terminated with exit code (137). Reason [OOMKilled]') == null
        with(TaskFailure.parse('nf-flyte task failed: exit=1\n--- stderr (tail)\n--- stdout (tail)\n')) {
            exitCode == 1
            stderr == ''
            stdout == ''
        }
    }

    def 'should write a failure back as Nextflow task files'() {
        when:
        new TaskFailure(3, 'boom\n', 'hello\n').writeTo(tmp)

        then:
        tmp.resolve(TaskRun.CMD_EXIT).text == '3\n'
        tmp.resolve(TaskRun.CMD_ERRFILE).text == 'boom\n'
        tmp.resolve(TaskRun.CMD_OUTFILE).text == 'hello\n'
    }

    def 'should render the wrapper with pod paths but write it to the real work dir'() {
        given:
        def workDir = Files.createDirectories(tmp.resolve('work/ab/cdef1234'))
        def bean = new TaskBean(name: 'FASTQC (1)', workDir: workDir, targetDir: workDir, script: 'fastqc reads.fq.gz\n',
            shell: ['/bin/bash', '-ue'], outputFiles: ['*.html'])
        def task = Mock(TaskRun) { toTaskBean() >> bean }
        def inputs = [
            new StagedInput('in_0', Path.of('/bucket/up/sample_1.fq.gz'), 'reads.fq.gz', false),
            new StagedInput('in_1', Path.of('/bucket/up/index'), 'ref/index', true),
        ]

        when:
        FlyteTaskLauncher.create(task, inputs).build()
        def wrapper = workDir.resolve('.command.run').text

        then: 'the files are written to the real work dir'
        workDir.resolve('.command.sh').text.contains('fastqc reads.fq.gz')

        and: 'the wrapper uses the pod task dir, never the real work dir'
        wrapper.contains('/var/outputs/task_dir/.command.sh')
        !wrapper.contains(workDir.toString())

        and: 'inputs are linked from where copilot puts them, keeping their Nextflow stage names'
        wrapper.contains('ln -s /var/inputs/in_0/sample_1.fq.gz reads.fq.gz')
        wrapper.contains('ln -s /var/inputs/in_1 ref/index')

        and: 'the task runs in a scratch dir and only declared outputs are copied to the task dir'
        wrapper.contains('NXF_SCRATCH="$(set +u; nxf_mktemp')
        wrapper.contains('nxf_fs_copy "$name" /var/outputs/task_dir')
        wrapper.contains('cp -fRL $source $target/$basedir')  // dereferences links
    }

    def 'should not leak a Nix head shell into the task container'() {
        expect:
        FlyteTaskLauncher.containerShell(['/nix/store/401wfz3g-bash-interactive-5.3p15/bin/bash', '-ue']) == ['/bin/bash', '-ue']
        FlyteTaskLauncher.containerShell(['/bin/bash', '-ue']) == ['/bin/bash', '-ue']
        FlyteTaskLauncher.containerShell(['/opt/conda/bin/bash', '-eu']) == ['/opt/conda/bin/bash', '-eu']
        FlyteTaskLauncher.containerShell(['bash']) == ['bash']
        FlyteTaskLauncher.containerShell(null) == null
    }

    def 'should render a Nix head shell as the container bash'() {
        given:
        def workDir = Files.createDirectories(tmp.resolve('work/cd/ef'))
        def bean = new TaskBean(name: 'X', workDir: workDir, targetDir: workDir, script: 'echo hi\n',
            shell: ['/nix/store/401wfz3g-bash-interactive-5.3p15/bin/bash', '-ue'])

        when:
        FlyteTaskLauncher.create(Mock(TaskRun) { toTaskBean() >> bean }, []).build()

        then:
        workDir.resolve('.command.sh').text.startsWith('#!/bin/bash -ue\n')
        workDir.resolve('.command.run').text.contains('/bin/bash -ue /var/outputs/task_dir/.command.sh')
        !workDir.resolve('.command.run').text.contains('/nix/store')
    }

    def 'should put staged files and dirs where copilot downloads them'() {
        expect:
        new StagedInput('in_0', Path.of('/b/x/sample_1.fq.gz'), 'reads.fq.gz', false).podPath == '/var/inputs/in_0/sample_1.fq.gz'
        new StagedInput('in_1', Path.of('/b/x/index'), 'index', true).podPath == '/var/inputs/in_1'
    }
}
