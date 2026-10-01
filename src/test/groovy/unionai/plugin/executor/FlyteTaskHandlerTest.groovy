package unionai.plugin.executor

import java.nio.file.Files
import java.nio.file.Path

import com.google.common.hash.HashCode
import nextflow.exception.ProcessException
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import spock.lang.Specification
import spock.lang.TempDir
import unionai.plugin.client.FlyteClient
import unionai.plugin.config.FlyteConfig

class FlyteTaskHandlerTest extends Specification {

    @TempDir
    Path workDir

    FlyteExecutor executor = Mock(FlyteExecutor)
    FlyteClient client = Mock(FlyteClient)

    def setup() {
        executor.getClient() >> client
        executor.getFlyteConfig() >> FlyteActionBuilderTest.config()
    }

    FlyteTaskHandler handler(TaskStatus status, Path dir = workDir) {
        def task = Mock(TaskRun) { getWorkDir() >> dir }
        def h = new FlyteTaskHandler(task, executor)
        h.actionName = 'nf-abc'
        h.status = status
        return h
    }

    def 'should not be running until the action has started'() {
        given:
        def h = handler(TaskStatus.SUBMITTED)

        when:
        def running = h.checkIfRunning()

        then:
        1 * executor.phaseOf('nf-abc') >> phase
        running == expected
        h.status == (expected ? TaskStatus.RUNNING : TaskStatus.SUBMITTED)

        where:
        phase                               | expected
        null                                | false  // enqueued but not readable yet
        'ACTION_PHASE_QUEUED'               | false
        'ACTION_PHASE_WAITING_FOR_RESOURCES'| false
        'ACTION_PHASE_INITIALIZING'         | false
        'ACTION_PHASE_RUNNING'              | true
        'ACTION_PHASE_SUCCEEDED'            | true   // finished between polls
        'ACTION_PHASE_FAILED'               | true
    }

    def 'should name a cacheable task after its cache key and attempt, so a resumed task keeps its name'() {
        given:
        def processor = Mock(TaskProcessor) { isCacheable() >> true }
        def task = Mock(TaskRun) {
            getProcessor() >> processor
            getFailCount() >> 1
            getHash() >> HashCode.fromString(hash)
        }

        expect: 'Nextflow gives a resumed in-flight task a new hash, but the name stays the same'
        FlyteTaskHandler.actionNameFor(task, 'a1b2c3d4e5f6a7b8c9d0e1f2a3b4') == 'nf-a1b2c3d4e5f6a7b8c9d0e1f2-1'

        where:
        hash << ['0123456789abcdef0123456789abcdef', 'fedcba9876543210fedcba9876543210']
    }

    def 'should name a non-cacheable task after its hash'() {
        given:
        def processor = Mock(TaskProcessor) { isCacheable() >> false }
        def task = Mock(TaskRun) {
            getProcessor() >> processor
            getHash() >> HashCode.fromString('0123456789abcdef0123456789abcdef')
        }

        expect:
        FlyteTaskHandler.actionNameFor(task, 'a1b2c3d4e5f6a7b8c9d0e1f2a3b4') == 'nf-0123456789abcdef01234567'
    }

    static Map outputs(Path taskDir) {
        [outputs: [literals: [[name: 'task_dir', value: [scalar: [blob: [uri: taskDir.toString()]]]]]]]
    }

    FlyteTaskHandler submittable(Map details) {
        def processor = Mock(TaskProcessor) { isCacheable() >> true }
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
            getContainer() >> 'ubuntu'
            getProcessor() >> processor
            getFailCount() >> 0
            getHash() >> HashCode.fromString('0123456789abcdef0123456789abcdef')
        }
        def h = Spy(FlyteTaskHandler, constructorArgs: [task, executor])
        h.cacheKey() >> 'a1b2c3d4e5f6a7b8c9d0e1f2a3b4'
        h.writeInputs() >> [:]
        h.newAction(_) >> { [name: h.@actionName] }
        client.getActionDetails([run: [org: 'acme', project: 'proj', domain: 'development', name: 'r123'], name: 'nf-a1b2c3d4e5f6a7b8c9d0e1f2-0']) >> details
        return h
    }

    def 'should attach to an action a previous attempt left running'() {
        given:
        def h = submittable([status: [phase: 'ACTION_PHASE_RUNNING']])

        when:
        h.submit()

        then:
        1 * client.enqueue([name: 'nf-a1b2c3d4e5f6a7b8c9d0e1f2-0']) >> false
        0 * client.enqueue(_)
        h.@actionName == 'nf-a1b2c3d4e5f6a7b8c9d0e1f2-0'
        h.status == TaskStatus.SUBMITTED
    }

    def 'should run the task again when a previous attempt aborted its action'() {
        given:
        def h = submittable([status: [phase: 'ACTION_PHASE_ABORTED']])

        when:
        h.submit()

        then:
        1 * client.enqueue([name: 'nf-a1b2c3d4e5f6a7b8c9d0e1f2-0']) >> false
        1 * client.enqueue([name: 'nf-0123456789abcdef01234567']) >> true
        h.@actionName == 'nf-0123456789abcdef01234567'
        h.status == TaskStatus.SUBMITTED
    }

    def 'should copy the task dir output into the work dir on success'() {
        given: "the action's task_dir output, as copilot uploaded it"
        def work = Files.createDirectories(workDir.resolve('ab/cdef'))
        def h = handler(TaskStatus.RUNNING, work)
        def taskDir = Files.createDirectories(workDir.resolve('flyte-output/task_dir'))
        Files.createDirectories(taskDir.resolve('sub'))
        taskDir.resolve(TaskRun.CMD_EXIT).text = '0\n'
        taskDir.resolve('report.html').text = '<html/>'
        taskDir.resolve('sub/x.txt').text = 'nested'

        when:
        def done = h.checkIfCompleted()

        then:
        1 * executor.phaseOf('nf-abc') >> 'ACTION_PHASE_SUCCEEDED'
        1 * client.getActionData([run: [org: 'acme', project: 'proj', domain: 'development', name: 'r123'], name: 'nf-abc']) >> outputs(taskDir)
        done
        1 * h.task.setExitStatus(0)
        0 * h.task.setError(_)
        work.resolve('report.html').text == '<html/>'
        work.resolve('sub/x.txt').text == 'nested'
        work.resolve(TaskRun.CMD_EXIT).text == '0\n'
    }

    def 'should flag a success without an exit file'() {
        given:
        def h = handler(TaskStatus.RUNNING)
        def taskDir = Files.createDirectories(workDir.resolve('empty-output'))

        when:
        h.checkIfCompleted()

        then:
        1 * executor.phaseOf('nf-abc') >> 'ACTION_PHASE_SUCCEEDED'
        1 * client.getActionData(_) >> outputs(taskDir)
        1 * h.task.setExitStatus(Integer.MAX_VALUE)
        1 * h.task.setError({ ProcessException e -> e.message.contains('no .exitcode') })
    }

    def 'should restore a failed task script as Nextflow task files'() {
        given:
        def h = handler(TaskStatus.RUNNING)

        when:
        h.checkIfCompleted()

        then: 'the failure the pod reported through copilot'
        1 * executor.phaseOf('nf-abc') >> 'ACTION_PHASE_FAILED'
        1 * client.getActionDetails(_) >> [errorInfo: [message: 'User Error: nf-flyte task failed: exit=3\n--- stderr (tail)\nboom\n--- stdout (tail)\nhello\n']]
        0 * client.getActionData(_)

        and: "Nextflow's errorStrategy sees a normal failed task"
        1 * h.task.setExitStatus(3)
        0 * h.task.setError(_)
        workDir.resolve(TaskRun.CMD_EXIT).text == '3\n'
        workDir.resolve(TaskRun.CMD_ERRFILE).text == 'boom\n'
        workDir.resolve(TaskRun.CMD_OUTFILE).text == 'hello\n'
    }

    def 'should report a failure when the wrapper never finished'() {
        given:
        def h = handler(TaskStatus.RUNNING)

        when:
        h.checkIfCompleted()

        then:
        1 * executor.phaseOf('nf-abc') >> 'ACTION_PHASE_FAILED'
        1 * client.getActionDetails([run: [org: 'acme', project: 'proj', domain: 'development', name: 'r123'], name: 'nf-abc']) >>
            [errorInfo: [message: 'Pod failed. [x] terminated with exit code (137). Reason [OOMKilled].']]
        1 * h.task.setExitStatus(137)
        1 * h.task.setError({ ProcessException e -> e.message.contains('OOMKilled') })
        h.status == TaskStatus.COMPLETED
    }

    def 'should use MAX_VALUE when no exit code is known'() {
        given:
        def h = handler(TaskStatus.RUNNING)

        when:
        h.checkIfCompleted()

        then:
        1 * executor.phaseOf('nf-abc') >> 'ACTION_PHASE_ABORTED'
        1 * client.getActionDetails(_) >> [abortInfo: [reason: 'user aborted']]
        1 * h.task.setExitStatus(Integer.MAX_VALUE)
        1 * h.task.setError({ ProcessException e -> e.message.contains('aborted: user aborted') })
    }

    def 'should keep waiting while the action is not terminal'() {
        given:
        def h = handler(TaskStatus.RUNNING)

        when:
        def done = h.checkIfCompleted()

        then:
        1 * executor.phaseOf('nf-abc') >> 'ACTION_PHASE_RUNNING'
        !done
        h.status == TaskStatus.RUNNING
    }

    def 'should abort the action on kill'() {
        given:
        def h = handler(TaskStatus.RUNNING)

        when:
        h.killTask()

        then:
        1 * client.abort([run: [org: 'acme', project: 'proj', domain: 'development', name: 'r123'], name: 'nf-abc'], 'Killed by Nextflow', 'a0')
    }

    def 'should name the inputs file the way the executor looks it up'() {
        expect: 'the executor reads <input_uri minus /inputs.pb>/inputs.pb, so any other name is never found'
        FlyteTaskHandler.INPUTS_FILE.endsWith('/inputs.pb')
    }

    def 'should parse exit codes from Flyte errors'() {
        expect:
        FlyteTaskHandler.exitCodeFromMessage(msg) == code

        where:
        msg                                                        | code
        '[a0] terminated with exit code (137). Reason [OOMKilled]' | 137
        'terminated with exit code (3). Reason [Error]'            | 3
        'image pull backoff'                                       | null
        null                                                       | null
    }
}
