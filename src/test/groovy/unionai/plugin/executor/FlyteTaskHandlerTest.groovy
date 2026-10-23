package unionai.plugin.executor

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

import com.google.common.hash.HashCode
import flyteidl2.actions.ActionsServiceOuterClass.Action
import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.common.Phase.ActionPhase
import flyteidl2.core.Execution.ExecutionError
import flyteidl2.task.Common.Outputs
import flyteidl2.workflow.RunDefinition.AbortInfo
import flyteidl2.workflow.RunDefinition.ActionDetails
import flyteidl2.workflow.RunServiceOuterClass.GetActionDataResponse
import nextflow.Session
import nextflow.exception.ProcessException
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import spock.lang.Specification
import spock.lang.TempDir
import unionai.flyte.controller.ActionController
import unionai.flyte.controller.ActionState
import unionai.flyte.controller.RunClient
import unionai.flyte.controller.Submitted
import unionai.plugin.client.InputsProto
import unionai.plugin.client.InputsProto.BlobInput
import unionai.plugin.config.FlyteConfig

class FlyteTaskHandlerTest extends Specification {

    @TempDir
    Path workDir

    FlyteExecutor executor = Mock(FlyteExecutor)
    ActionController controller = Mock(ActionController)
    RunClient runs = Mock(RunClient)
    CodeBundles codeBundles = Mock(CodeBundles) { upload(_) >> CompletableFuture.completedFuture('s3://fast/abc/code.tar.gz') }
    FlyteConfig flyteConfig = FlyteActionBuilderTest.config()

    def setup() {
        executor.getController() >> controller
        executor.getRuns() >> runs
        executor.getCodeBundles() >> codeBundles
        executor.getFlyteConfig() >> { flyteConfig }
    }

    static ActionState state(String phase, String outputUri = '', String error = null) {
        new ActionState('nf-abc', ActionPhase.valueOf(phase), error ? ExecutionError.newBuilder().setMessage(error).build() : null, outputUri)
    }

    static CompletableFuture<Submitted> done(Submitted result) {
        CompletableFuture.completedFuture(result)
    }

    FlyteTaskHandler handler(TaskStatus status, Path dir = workDir) {
        def task = Mock(TaskRun) { getWorkDir() >> dir }
        def h = new FlyteTaskHandler(task, executor)
        h.actionName = 'nf-abc'
        h.status = status
        h.submission = done(Submitted.CREATED)
        return h
    }

    def 'should copy a task dir with nested directories into a local work dir'() {
        given:
        def source = workDir.resolve('task_dir')
        Files.createDirectories(source.resolve('bbsplit_index/ref/genome/1'))
        source.resolve('bbsplit_index/ref/genome/1/info.txt').text = 'info'
        source.resolve('Coverage Profile Along Genes (High).png').text = 'png'
        source.resolve('out.txt').text = 'out'
        def target = workDir.resolve('work/ab/cdef')

        when:
        FlyteTaskHandler.copyTree(source, target)

        then:
        target.resolve('bbsplit_index/ref/genome/1/info.txt').text == 'info'
        target.resolve('out.txt').text == 'out'
        target.resolve('Coverage Profile Along Genes (High).png').text == 'png'
    }

    def 'should not be running until the action has started'() {
        given:
        def h = handler(TaskStatus.SUBMITTED)

        when:
        def running = h.checkIfRunning()

        then:
        1 * controller.state('nf-abc') >> (phase ? state(phase) : null)
        running == expected
        h.status == (expected ? TaskStatus.RUNNING : TaskStatus.SUBMITTED)

        where:
        phase                               | expected
        null                                | false  // enqueued but not reported by the watch yet
        'ACTION_PHASE_QUEUED'               | false
        'ACTION_PHASE_WAITING_FOR_RESOURCES'| false
        'ACTION_PHASE_INITIALIZING'         | false
        'ACTION_PHASE_RUNNING'              | true
        'ACTION_PHASE_SUCCEEDED'            | true   // finished before the monitor looked
        'ACTION_PHASE_FAILED'               | true
    }

    def 'should wait for the enqueue to complete'() {
        given:
        def h = handler(TaskStatus.SUBMITTED)
        h.submission = new CompletableFuture<Submitted>()

        when:
        def running = h.checkIfRunning()

        then:
        !running
        0 * controller.state(_)
    }

    def 'should fail the task when the enqueue failed'() {
        given:
        def h = handler(TaskStatus.SUBMITTED)
        h.submission = CompletableFuture.failedFuture(new IllegalStateException('INVALID_ARGUMENT: bad spec'))

        when:
        h.checkIfRunning()

        then:
        def e = thrown(ProcessException)
        e.message.contains('bad spec')
    }

    def 'should key the cache on the env and eval outputs a cached .command.env holds'() {
        given:
        executor.getCacheKeys() >> new FlyteCacheKey(workDir)
        executor.getBinHash() >> ''
        executor.getSession() >> Mock(Session) { getStubRun() >> false }
        def key = { List<String> envs, Map<String,String> evals ->
            def task = Mock(TaskRun) {
                getProcessor() >> Mock(TaskProcessor) { getName() >> 'GUNZIP' }
                getScript() >> 'gzip -cd x.gz > x'
                getContainer() >> 'ubuntu'
                getWorkDir() >> Files.createTempDirectory(workDir, 'task')
                getInputFilesMap() >> [:]
                getOutputEnvNames() >> envs
                getOutputEvals() >> evals
            }
            new FlyteTaskHandler(task, executor).cacheKey()
        }

        expect: 'the same outputs, the same key'
        key([], [nxf_out_eval_82: 'gzip --version']) == key([], [nxf_out_eval_82: 'gzip --version'])

        and: 'renumbered by another pipeline revision or Nextflow version: a different key'
        key([], [nxf_out_eval_82: 'gzip --version']) != key([], [nxf_out_eval_154: 'gzip --version'])
        key([], [nxf_out_eval_82: 'gzip --version']) != key([], [nxf_out_eval_82: 'gzip -V'])
        key(['FOO'], [:]) != key(['BAR'], [:])
        key([], [:]) != key(['FOO'], [:])
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

    FlyteTaskHandler submittable() {
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
        h.storeInputs(_) >> { String name -> "s3://bucket/base/$name/inputs.pb".toString() }
        h.newAction(_) >> { Action.newBuilder().setActionId(ActionIdentifier.newBuilder().setName(h.@actionName)).build() }
        return h
    }

    static boolean named(Action action, String name) {
        action.actionId.name == name
    }

    def 'should attach to an action a previous attempt left running'() {
        given:
        def h = submittable()

        when:
        h.submit()
        def running = h.checkIfRunning()

        then:
        1 * controller.submit({ named(it, 'nf-a1b2c3d4e5f6a7b8c9d0e1f2-0') }) >> done(Submitted.EXISTS)
        0 * controller.submit(_)
        _ * controller.state('nf-a1b2c3d4e5f6a7b8c9d0e1f2-0') >> state('ACTION_PHASE_RUNNING')
        running
        h.@actionName == 'nf-a1b2c3d4e5f6a7b8c9d0e1f2-0'
        h.status == TaskStatus.RUNNING
    }

    def 'should run the task again when a previous attempt aborted its action'() {
        given:
        def h = submittable()

        when:
        h.submit()
        def running = h.checkIfRunning()

        then:
        1 * controller.submit({ named(it, 'nf-a1b2c3d4e5f6a7b8c9d0e1f2-0') }) >> done(Submitted.EXISTS)
        _ * controller.state('nf-a1b2c3d4e5f6a7b8c9d0e1f2-0') >> state('ACTION_PHASE_ABORTED')
        1 * controller.submit({ named(it, 'nf-0123456789abcdef01234567') }) >> done(Submitted.CREATED)
        !running
        h.@actionName == 'nf-0123456789abcdef01234567'
        h.status == TaskStatus.SUBMITTED

        when: 'the new action starts'
        running = h.checkIfRunning()

        then:
        1 * controller.state('nf-0123456789abcdef01234567') >> state('ACTION_PHASE_RUNNING')
        0 * controller.submit(_)
        running
    }

    static void writeOutputs(Path dir, Path taskDir) {
        Files.createDirectories(dir)
        dir.resolve(FlyteTaskHandler.OUTPUTS_FILE).bytes = InputsProto.encode([new BlobInput('task_dir', taskDir.toString(), true)])
    }

    static Outputs outputs(Path taskDir) {
        Outputs.parseFrom(InputsProto.encode([new BlobInput('task_dir', taskDir.toString(), true)]))
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
        writeOutputs(workDir.resolve('flyte-output'), taskDir)

        when:
        def done = h.checkIfCompleted()

        then: 'the outputs are read where the watch says they are'
        1 * controller.state('nf-abc') >> state('ACTION_PHASE_SUCCEEDED', workDir.resolve('flyte-output').toString())
        0 * runs._
        done
        1 * h.task.setExitStatus(0)
        0 * h.task.setError(_)
        work.resolve('report.html').text == '<html/>'
        work.resolve('sub/x.txt').text == 'nested'
        work.resolve(TaskRun.CMD_EXIT).text == '0\n'
    }

    def 'should ask Flyte for the outputs when the watch has no output location'() {
        given:
        def work = Files.createDirectories(workDir.resolve('ab/cdef'))
        def h = handler(TaskStatus.RUNNING, work)
        def taskDir = Files.createDirectories(workDir.resolve('flyte-output/task_dir'))
        taskDir.resolve(TaskRun.CMD_EXIT).text = '0\n'

        when:
        h.checkIfCompleted()

        then:
        1 * controller.state('nf-abc') >> state(phase)
        1 * runs.getActionData(FlyteActionBuilder.actionId(FlyteActionBuilderTest.config(), 'nf-abc')) >>
            GetActionDataResponse.newBuilder().setOutputs(outputs(taskDir)).build()
        1 * h.task.setExitStatus(0)

        where:
        phase << ['ACTION_PHASE_SUCCEEDED', 'ACTION_PHASE_RECOVERED']
    }

    def 'should flag a success without an exit file'() {
        given:
        def h = handler(TaskStatus.RUNNING)
        def taskDir = Files.createDirectories(workDir.resolve('empty-output'))

        when:
        h.checkIfCompleted()

        then:
        1 * controller.state('nf-abc') >> state('ACTION_PHASE_SUCCEEDED')
        1 * runs.getActionData(_) >> GetActionDataResponse.newBuilder().setOutputs(outputs(taskDir)).build()
        1 * h.task.setExitStatus(Integer.MAX_VALUE)
        1 * h.task.setError({ ProcessException e -> e.message.contains('no .exitcode') })
    }

    def 'should restore a failed task script as Nextflow task files'() {
        given:
        def h = handler(TaskStatus.RUNNING)

        when:
        h.checkIfCompleted()

        then: 'the failure the pod reported through copilot, as the watch delivered it'
        1 * controller.state('nf-abc') >> state('ACTION_PHASE_FAILED', '', 'User Error: nf-flyte task failed: exit=3\n--- stderr (tail)\nboom\n--- stdout (tail)\nhello\n')
        0 * runs._

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
        1 * controller.state('nf-abc') >> state('ACTION_PHASE_FAILED', '', 'Pod failed. [x] terminated with exit code (137). Reason [OOMKilled].')
        1 * h.task.setExitStatus(137)
        1 * h.task.setError({ ProcessException e -> e.message.contains('OOMKilled') })
        h.status == TaskStatus.COMPLETED
    }

    def 'should use MAX_VALUE when no exit code is known'() {
        given:
        def h = handler(TaskStatus.RUNNING)

        when:
        h.checkIfCompleted()

        then: 'an abort has no error: the reason comes from the action details'
        1 * controller.state('nf-abc') >> state('ACTION_PHASE_ABORTED')
        1 * runs.getActionDetails(FlyteActionBuilder.actionId(FlyteActionBuilderTest.config(), 'nf-abc')) >>
            ActionDetails.newBuilder().setAbortInfo(AbortInfo.newBuilder().setReason('user aborted')).build()
        1 * h.task.setExitStatus(Integer.MAX_VALUE)
        1 * h.task.setError({ ProcessException e -> e.message.contains('aborted: user aborted') })
    }

    def 'should keep waiting while the action is not terminal'() {
        given:
        def h = handler(TaskStatus.RUNNING)

        when:
        def done = h.checkIfCompleted()

        then:
        1 * controller.state('nf-abc') >> state('ACTION_PHASE_RUNNING')
        !done
        h.status == TaskStatus.RUNNING
    }

    def 'should abort the action on kill'() {
        given:
        def h = handler(TaskStatus.RUNNING)

        when:
        h.killTask()

        then:
        1 * controller.abort('nf-abc', 'Killed by Nextflow') >> CompletableFuture.completedFuture(null)
    }

    def 'should not run a killed task again'() {
        given: 'a resumed task whose action a previous attempt aborted'
        controller.submit(_) >> done(Submitted.EXISTS)
        controller.state(_) >> state('ACTION_PHASE_ABORTED')
        def h = submittable()
        h.submit()

        when: 'Nextflow kills it before the monitor sees the enqueue result'
        h.killTask()
        h.checkIfRunning()

        then:
        0 * controller.submit({ named(it, 'nf-0123456789abcdef01234567') })
        h.@actionName == 'nf-a1b2c3d4e5f6a7b8c9d0e1f2-0'
    }

    def 'should submit without waiting for the script upload, and enqueue once it is done'() {
        given:
        def upload = new CompletableFuture<String>()
        def h = submittable()

        when:
        h.submit()

        then:
        1 * codeBundles.upload(workDir.resolve('.command.sh')) >> upload
        0 * controller.submit(_)
        h.status == TaskStatus.SUBMITTED
        !h.checkIfRunning()

        when:
        upload.complete('s3://fast/abc/code.tar.gz')

        then:
        1 * controller.submit({ named(it, 'nf-a1b2c3d4e5f6a7b8c9d0e1f2-0') }) >> done(Submitted.CREATED)
        h.@codeBundleUri == 's3://fast/abc/code.tar.gz'
    }

    def 'should abort a task killed while its script uploads once it is enqueued'() {
        given:
        def upload = new CompletableFuture<String>()
        def h = submittable()

        when:
        h.submit()
        h.killTask()

        then:
        1 * codeBundles.upload(_) >> upload
        0 * controller.submit(_)
        0 * controller.abort(_, _)

        when:
        upload.complete(null)

        then: 'without code view, the action is enqueued, then aborted'
        1 * controller.submit(_) >> done(Submitted.CREATED)
        1 * controller.abort('nf-a1b2c3d4e5f6a7b8c9d0e1f2-0', 'Killed by Nextflow') >> CompletableFuture.completedFuture(null)
    }

    def 'should abort a task killed while enqueuing once the enqueue is done'() {
        given:
        def h = handler(TaskStatus.SUBMITTED)
        def enqueue = new CompletableFuture<Submitted>()
        h.submission = enqueue

        when:
        h.killTask()

        then:
        0 * controller.abort(_, _)

        when:
        enqueue.complete(Submitted.CREATED)

        then:
        1 * controller.abort('nf-abc', 'Killed by Nextflow') >> CompletableFuture.completedFuture(null)
    }

    TaskRun hashedTask(Path work) {
        Mock(TaskRun) {
            getWorkDir() >> work
            getHash() >> HashCode.fromString('0123456789abcdef0123456789abcdef')
        }
    }

    def 'should store inputs under the run, where Flyte keeps action data'() {
        given:
        def base = Files.createDirectories(workDir.resolve('run-base'))
        def work = Files.createDirectories(workDir.resolve('ab/cdef'))
        flyteConfig = FlyteActionBuilderTest.config(runOutputBase: base.toString() + '/')
        def h = new FlyteTaskHandler(hashedTask(work), executor)
        h.@inputsBytes = [1, 2, 3] as byte[]

        when:
        def uri = h.storeInputs('nf-abc-0')

        then: 'in a directory of its own, so a resumed head attaching to the action keeps its inputs'
        def stored = base.resolve('nf-abc-0/0123456789abcdef0123456789abcdef/inputs.pb')
        uri == stored.toString()
        stored.bytes == [1, 2, 3] as byte[]
        !Files.exists(work.resolve(FlyteTaskHandler.INPUTS_FILE))
    }

    def 'should store inputs in the work dir once the run storage proved not writable'() {
        given: 'a run base below a regular file, so nothing can be written there'
        def blocker = workDir.resolve('blocker')
        blocker.text = 'x'
        def work = Files.createDirectories(workDir.resolve('ab/cdef'))
        flyteConfig = FlyteActionBuilderTest.config(runOutputBase: blocker.resolve('base').toString())
        def h = new FlyteTaskHandler(hashedTask(work), executor)
        h.@inputsBytes = [4, 5] as byte[]

        when:
        h.storeInputs('nf-abc-0')

        then:
        1 * executor.setInputsInWorkDir(true)
        work.resolve(FlyteTaskHandler.INPUTS_FILE).bytes == [4, 5] as byte[]

        when: 'later tasks skip the run storage'
        h.storeInputs('nf-def-0')

        then:
        _ * executor.isInputsInWorkDir() >> true
        0 * executor.setInputsInWorkDir(_)
    }

    def 'should name the inputs file the way the executor looks it up'() {
        expect: 'the executor reads <input_uri minus /inputs.pb>/inputs.pb, so any other name is never found'
        FlyteTaskHandler.INPUTS_FILE.endsWith('/inputs.pb')
        FlyteTaskHandler.INPUTS_NAME == 'inputs.pb'
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
