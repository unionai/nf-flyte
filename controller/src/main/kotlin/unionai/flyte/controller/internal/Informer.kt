package unionai.flyte.controller.internal

import com.connectrpc.Code
import com.connectrpc.Headers
import flyteidl2.actions.ActionsServiceClientInterface
import flyteidl2.actions.ActionsServiceOuterClass.WatchForUpdatesRequest
import flyteidl2.actions.ActionsServiceOuterClass.WatchForUpdatesResponse
import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.workflow.StateServiceOuterClass.ActionUpdate
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import unionai.flyte.controller.ActionState
import unionai.flyte.controller.FlyteException
import unionai.flyte.controller.TokenSource

/**
 * Keeps the state of every child action of one parent action in memory, from the Actions
 * service's `WatchForUpdates` stream.
 *
 * The watch is a list-watch: it first sends the current state of every child, then a sentinel,
 * then each change as it happens. A reconnect sends everything again, so after any failure the
 * cache converges back to the service's state without a separate list call. For the same reason
 * the watch is reopened every [resyncMillis]: a stream that stalls without failing (e.g. behind
 * a proxy) can't hold back updates for longer than that.
 */
internal class Informer(
    private val actions: ActionsServiceClientInterface,
    private val parent: ActionIdentifier,
    private val headers: Headers,
    private val tokens: TokenSource?,
    private val listener: (ActionState) -> Unit,
    private val minBackoffMillis: Long = 1_000,
    private val maxBackoffMillis: Long = 30_000,
    /** Consecutive failed watches before giving up: about 20 minutes at the 30s backoff cap */
    private val maxFailures: Int = 40,
    private val resyncMillis: Long = 5 * 60_000,
) {
    private val states = ConcurrentHashMap<String, ActionState>()
    private val synced = CompletableDeferred<Unit>()

    /** Why the watch gave up, after [maxFailures] failed reconnects in a row */
    @Volatile var failure: Throwable? = null
        private set

    operator fun get(name: String): ActionState? = states[name]

    val size: Int get() = states.size

    /** Wait until the watch has delivered the state every child action had when it started */
    suspend fun awaitSynced() = synced.await()

    /**
     * Watch until cancelled, reconnecting with exponential backoff. A watch that fails, or ends
     * without delivering anything, counts as a failure; one that delivered updates resets the count.
     */
    suspend fun run() {
        var failures = 0
        while (true) {
            var delivered = false
            val error = try {
                val ended = withTimeoutOrNull(resyncMillis) { watch { delivered = true }; true }
                if (ended == null) {
                    // the resync interval elapsed: reopen the watch right away
                    failures = 0
                    continue
                }
                FlyteException("the server ended the watch")
            }
            catch (e: CancellationException) {
                throw e
            }
            catch (e: Exception) {
                e
            }
            // the stream reported it inside a 200 response, past the auth interceptor
            if (error.connectCode == Code.UNAUTHENTICATED) tokens?.let { runCatching { it.invalidate(it.token()) } }
            failures = if (delivered) 1 else failures + 1
            if (failures > maxFailures) {
                log.error("[FLYTE] watch of {} failed {} times in a row, giving up", parent.name, maxFailures, error)
                failure = error
                synced.completeExceptionally(error)
                return
            }
            log.warn("[FLYTE] watch of {} ended ({}), reconnecting (attempt {}/{})", parent.name, error.message ?: error.javaClass.simpleName, failures, maxFailures)
            delay(backoffMillis(failures, minBackoffMillis, maxBackoffMillis))
        }
    }

    private suspend fun watch(onMessage: () -> Unit) {
        val stream = actions.watchForUpdates(headers)
        try {
            stream.sendAndClose(WatchForUpdatesRequest.newBuilder().setParentActionId(parent).build()).getOrThrow()
            for (resp in stream.responseChannel()) {
                onMessage()
                when (resp.messageCase) {
                    WatchForUpdatesResponse.MessageCase.CONTROL_MESSAGE ->
                        if (resp.controlMessage.sentinel && synced.complete(Unit))
                            log.debug("[FLYTE] watch of {} synced, {} existing actions", parent.name, states.size)
                    WatchForUpdatesResponse.MessageCase.ACTION_UPDATE -> observe(resp.actionUpdate)
                    else -> log.trace("[FLYTE] ignoring watch message {}", resp.messageCase)
                }
            }
        }
        finally {
            // also when cancelled, or the HTTP stream stays open
            withContext(NonCancellable) { stream.receiveClose() }
        }
    }

    internal fun observe(update: ActionUpdate) {
        val next = ActionState.of(update)
        var changed = false
        states.compute(next.name) { _, prev ->
            when {
                // a terminal phase is final: ignore a stale update replayed after it
                prev != null && prev.isTerminal && !next.isTerminal -> prev
                prev != null && prev.phase == next.phase && prev.outputUri == next.outputUri && prev.error == next.error -> prev
                else -> next.also { changed = true }
            }
        }
        if (changed) {
            log.trace("[FLYTE] {}", next)
            listener(next)
        }
    }

    private companion object {
        val log = LoggerFactory.getLogger(Informer::class.java)
    }
}
