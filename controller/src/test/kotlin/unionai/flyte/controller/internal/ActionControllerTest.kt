package unionai.flyte.controller.internal

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.ResponseMessage
import flyteidl2.actions.ActionsServiceOuterClass.Action
import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.common.Phase.ActionPhase
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import unionai.flyte.controller.ActionListener
import unionai.flyte.controller.ActionState
import unionai.flyte.controller.FlyteException
import unionai.flyte.controller.Submitted

class ActionControllerTest {

    private val actions = FakeActions()
    private val controller = DefaultActionController(
        actions, RUN, "a0", tokens = null,
        enqueues = Caller(Retry(maxAttempts = 3, minBackoffMillis = 1, maxBackoffMillis = 1)),
        aborts = Caller(Retry(maxAttempts = 3, minBackoffMillis = 1, maxBackoffMillis = 1)),
    )

    @AfterTest
    fun close() = controller.close()

    private fun action(name: String): Action = Action.newBuilder()
        .setActionId(ActionIdentifier.newBuilder().setRun(RUN).setName(name))
        .setParentActionName("a0")
        .setInputUri("s3://b/in")
        .setRunOutputBase("s3://b/out")
        .build()

    private fun started(vararg existing: String) {
        val watch = actions.newWatch()
        existing.forEach { watch.trySend(update(it, ActionPhase.ACTION_PHASE_ABORTED)) }
        watch.trySend(SENTINEL)
        controller.start(Duration.ofSeconds(5))
    }

    @Test
    fun `start waits for the watch to list existing actions`() {
        started("nf-old")
        assertEquals(ActionPhase.ACTION_PHASE_ABORTED, controller.state("nf-old")?.phase)
        assertNull(controller.state("nf-new"))
    }

    @Test
    fun `start fails when the watch does not sync in time`() {
        actions.newWatch()
        assertFailsWith<FlyteException> { controller.start(Duration.ofMillis(100)) }
    }

    @Test
    fun `enqueues with the routing headers`() {
        started()
        assertEquals(Submitted.CREATED, controller.submit(action("nf-a")).get(5, TimeUnit.SECONDS))

        val (request, headers) = actions.enqueued.single()
        assertEquals("nf-a", request.action.actionId.name)
        assertEquals(
            mapOf(
                "x-actions-project" to listOf("proj"),
                "x-actions-domain" to listOf("development"),
                "x-actions-run" to listOf("r1"),
                "x-actions-parent-action" to listOf("a0"),
            ),
            headers,
        )
    }

    @Test
    fun `reports an existing action instead of failing`() {
        started()
        actions.enqueueResults.add(Code.ALREADY_EXISTS)
        assertEquals(Submitted.EXISTS, controller.submit(action("nf-a")).get(5, TimeUnit.SECONDS))
    }

    @Test
    fun `retries transient enqueue failures`() {
        started()
        actions.enqueueResults.addAll(listOf(Code.UNAVAILABLE, Code.FAILED_PRECONDITION))
        assertEquals(Submitted.CREATED, controller.submit(action("nf-a")).get(5, TimeUnit.SECONDS))
        assertEquals(3, actions.enqueued.size)
    }

    @Test
    fun `fails the submit on a permanent error`() {
        started()
        actions.enqueueResults.add(Code.INVALID_ARGUMENT)
        val e = assertFailsWith<ExecutionException> { controller.submit(action("nf-a")).get(5, TimeUnit.SECONDS) }
        assertIs<FlyteException>(e.cause)
        assertEquals(1, actions.enqueued.size)
    }

    @Test
    fun `gives up after the retry budget`() {
        started()
        repeat(3) { actions.enqueueResults.add(Code.UNAVAILABLE) }
        assertFailsWith<ExecutionException> { controller.submit(action("nf-a")).get(5, TimeUnit.SECONDS) }
        assertEquals(3, actions.enqueued.size)
    }

    @Test
    fun `aborts, ignoring finished and missing actions`() {
        started("nf-done")
        controller.abort("nf-done", "killed").get(5, TimeUnit.SECONDS)
        assertTrue(actions.aborted.isEmpty())

        actions.abortResults.add(Code.NOT_FOUND)
        controller.abort("nf-gone", "killed").get(5, TimeUnit.SECONDS)
        controller.abort("nf-running", "killed").get(5, TimeUnit.SECONDS)
        assertEquals(listOf("nf-gone", "nf-running"), actions.aborted.map { it.actionId.name })
        assertEquals("killed", actions.aborted.last().reason)
    }

    @Test
    fun `notifies listeners of changes`() {
        val watch = actions.newWatch()
        watch.trySend(SENTINEL)
        val seen = java.util.concurrent.LinkedBlockingQueue<ActionState>()
        controller.addListener { seen.add(it) }
        controller.start(Duration.ofSeconds(5))

        watch.trySend(update("nf-a", ActionPhase.ACTION_PHASE_SUCCEEDED, "s3://b/a"))
        val state = seen.poll(5, TimeUnit.SECONDS)
        assertEquals("nf-a", state?.name)
        assertTrue(state!!.isSucceeded && state.isTerminal && state.isStarted)
    }

    @Test
    fun `retries an abort that reached the wrong shard`() {
        started()
        actions.abortResults.add(Code.FAILED_PRECONDITION)
        controller.abort("nf-running", "killed").get(5, TimeUnit.SECONDS)
        assertEquals(2, actions.aborted.size)
    }

    @Test
    fun `stops retrying an abort once the action finished`() {
        val watch = actions.newWatch()
        watch.trySend(SENTINEL)
        controller.start(Duration.ofSeconds(5))
        // the action finished before the abort reached it: the service keeps answering FAILED_PRECONDITION
        actions.abortResults.add(Code.FAILED_PRECONDITION)
        watch.trySend(update("nf-a", ActionPhase.ACTION_PHASE_SUCCEEDED))
        while (controller.state("nf-a") == null) Thread.sleep(1)

        controller.abort("nf-a", "killed").get(5, TimeUnit.SECONDS)
        assertTrue(actions.aborted.size <= 1)
    }

    @Test
    fun `keeps retrying an overloaded service only when asked to`() = runBlocking {
        var calls = 0
        val busy: suspend () -> ResponseMessage<String> = {
            calls++
            if (calls <= 4) ResponseMessage.Failure(ConnectException(Code.RESOURCE_EXHAUSTED), emptyMap(), emptyMap())
            else ResponseMessage.Success("ok", emptyMap(), emptyMap())
        }
        assertEquals("ok", Caller(Retry(maxAttempts = 2, minBackoffMillis = 1, untilAvailable = true)).unary("Test", call = busy).getOrThrow())

        calls = 0
        assertFailsWith<FlyteException> { Caller(Retry(maxAttempts = 2, minBackoffMillis = 1)).unary("Test", call = busy) }
        assertEquals(2, calls)
    }

    @Test
    fun `tells listeners when the watch gives up`() {
        val controller = DefaultActionController(actions, RUN, "a0", tokens = null, maxWatchFailures = 1)
        val failed = java.util.concurrent.CompletableFuture<Throwable>()
        val removed = ActionListener { throw AssertionError("removed listener called") }
        controller.addListener(object : ActionListener {
            override fun onUpdate(state: ActionState) {}
            override fun onWatchFailed(error: Throwable) { failed.complete(error) }
        })
        controller.addListener(removed)
        controller.removeListener(removed)
        val watch = actions.newWatch()
        watch.trySend(SENTINEL)
        watch.close(ConnectException(Code.PERMISSION_DENIED, "no"))
        actions.newWatch().close(ConnectException(Code.PERMISSION_DENIED, "no"))
        controller.use {
            it.start(Duration.ofSeconds(5))
            assertIs<ConnectException>(failed.get(10, TimeUnit.SECONDS))
            assertFailsWith<FlyteException> { it.state("nf-a") }
        }
    }
}
