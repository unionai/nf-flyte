package unionai.flyte.controller

import com.connectrpc.Code
import com.connectrpc.ConnectException
import flyteidl2.actions.ActionsServiceOuterClass.WatchForUpdatesResponse
import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.core.Execution.ExecutionError
import flyteidl2.common.Phase.ActionPhase
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield

class InformerTest {

    private val actions = FakeActions()
    private val seen = CopyOnWriteArrayList<ActionState>()

    private fun informer(maxFailures: Int = 40, resyncMillis: Long = 60_000) = Informer(
        actions = actions,
        parent = ActionIdentifier.newBuilder().setRun(RUN).setName("a0").build(),
        headers = mapOf("x-actions-run" to listOf("r1")),
        tokens = null,
        listener = { seen.add(it) },
        minBackoffMillis = 1,
        maxBackoffMillis = 5,
        maxFailures = maxFailures,
        resyncMillis = resyncMillis,
    )

    private suspend fun eventually(check: () -> Boolean) = withTimeout(5_000) {
        while (!check()) yield()
    }

    @Test
    fun `lists existing actions, then syncs on the sentinel`() = runBlocking {
        val watch = actions.newWatch()
        val informer = informer()
        val job = launch { informer.run() }

        watch.send(update("nf-a", ActionPhase.ACTION_PHASE_SUCCEEDED, "s3://b/a"))
        watch.send(update("nf-b", ActionPhase.ACTION_PHASE_RUNNING))
        watch.send(SENTINEL)
        withTimeout(5_000) { informer.awaitSynced() }

        assertEquals(ActionPhase.ACTION_PHASE_SUCCEEDED, informer["nf-a"]?.phase)
        assertEquals("s3://b/a", informer["nf-a"]?.outputUri)
        assertEquals(ActionPhase.ACTION_PHASE_RUNNING, informer["nf-b"]?.phase)
        assertNull(informer["nf-c"])
        assertEquals("a0", actions.watchRequests.single().first.parentActionId.name)
        assertEquals(listOf("r1"), actions.watchRequests.single().second["x-actions-run"])

        watch.send(update("nf-b", ActionPhase.ACTION_PHASE_FAILED))
        eventually { informer["nf-b"]?.phase == ActionPhase.ACTION_PHASE_FAILED }
        assertEquals(listOf("nf-a", "nf-b", "nf-b"), seen.map { it.name })
        job.cancelAndJoin()
    }

    @Test
    fun `reconnects after a failed watch and converges on the relisted state`() = runBlocking {
        val first = actions.newWatch()
        val informer = informer()
        val job = launch { informer.run() }

        first.send(update("nf-a", ActionPhase.ACTION_PHASE_RUNNING))
        first.send(SENTINEL)
        first.close(ConnectException(Code.UNAVAILABLE, "connection reset"))

        // the new watch lists everything again: nf-a finished while disconnected
        val second = actions.newWatch()
        second.send(update("nf-a", ActionPhase.ACTION_PHASE_SUCCEEDED, "s3://b/a"))
        second.send(update("nf-b", ActionPhase.ACTION_PHASE_QUEUED))
        second.send(SENTINEL)

        eventually { informer["nf-b"] != null }
        assertEquals(ActionPhase.ACTION_PHASE_SUCCEEDED, informer["nf-a"]?.phase)
        assertEquals(2, actions.watchRequests.size)
        job.cancelAndJoin()
    }

    @Test
    fun `only reports changes, and never moves a finished action back`() = runBlocking {
        val watch = actions.newWatch()
        val informer = informer()
        val job = launch { informer.run() }

        watch.send(update("nf-a", ActionPhase.ACTION_PHASE_RUNNING))
        watch.send(update("nf-a", ActionPhase.ACTION_PHASE_RUNNING))
        watch.send(update("nf-a", ActionPhase.ACTION_PHASE_SUCCEEDED))
        watch.send(update("nf-a", ActionPhase.ACTION_PHASE_RUNNING))
        watch.send(SENTINEL)
        withTimeout(5_000) { informer.awaitSynced() }

        assertEquals(ActionPhase.ACTION_PHASE_SUCCEEDED, informer["nf-a"]?.phase)
        assertEquals(listOf(ActionPhase.ACTION_PHASE_RUNNING, ActionPhase.ACTION_PHASE_SUCCEEDED), seen.map { it.phase })
        job.cancelAndJoin()
    }

    @Test
    fun `gives up after too many failed watches in a row`() = runBlocking {
        repeat(3) { actions.newWatch().close(ConnectException(Code.UNAVAILABLE, "down")) }
        val informer = informer(maxFailures = 2)

        withTimeout(5_000) { informer.run() }

        assertNotNull(informer.failure)
        assertFailsWith<ConnectException> { informer.awaitSynced() }
        assertTrue(actions.watchRequests.size == 3)
    }

    @Test
    fun `gives up when the server keeps ending the watch without sending anything`() = runBlocking {
        repeat(3) { actions.newWatch().close() }
        val informer = informer(maxFailures = 2)

        withTimeout(5_000) { informer.run() }

        assertIs<FlyteException>(informer.failure)
    }

    @Test
    fun `reopens a quiet watch, relisting what it missed`() = runBlocking {
        val first = actions.newWatch()
        first.send(update("nf-a", ActionPhase.ACTION_PHASE_RUNNING))
        first.send(SENTINEL)
        // the first stream stalls; the reopened one lists the action as done
        val second = actions.newWatch()
        second.send(update("nf-a", ActionPhase.ACTION_PHASE_SUCCEEDED))
        val informer = informer(resyncMillis = 50)
        val job = launch { informer.run() }

        eventually { informer["nf-a"]?.phase == ActionPhase.ACTION_PHASE_SUCCEEDED }
        assertEquals(2, actions.watchRequests.size)
        assertNull(informer.failure)
        job.cancelAndJoin()
    }

    @Test
    fun `reports an error that arrives after the phase`() = runBlocking {
        val watch = actions.newWatch()
        val informer = informer()
        val job = launch { informer.run() }

        watch.send(update("nf-a", ActionPhase.ACTION_PHASE_FAILED))
        watch.send(WatchForUpdatesResponse.newBuilder().setActionUpdate(
            update("nf-a", ActionPhase.ACTION_PHASE_FAILED).actionUpdate.toBuilder().setError(ExecutionError.newBuilder().setMessage("boom"))
        ).build())
        eventually { informer["nf-a"]?.error?.message == "boom" }
        assertEquals(2, seen.size)
        job.cancelAndJoin()
    }
}
