package unionai.flyte.controller

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
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory

/**
 * Keeps the state of every child action of one parent action in memory, from the Actions
 * service's `WatchForUpdates` stream.
 *
 * The watch is a list-watch: it first sends the current state of every child, then a sentinel,
 * then each change as it happens. A reconnect sends everything again, so after any failure the
 * cache converges back to the service's state without a separate list call.
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
     * Watch until cancelled, reconnecting with exponential backoff.
     */
    suspend fun run() {
        var failures = 0
        while (true) {
            try {
                watch { failures = 0 }
                // the server ended the stream (e.g. a deploy): reconnect, it lists everything again
                log.debug("[FLYTE] watch of {} ended, reconnecting", parent.name)
                failures++
            }
            catch (e: CancellationException) {
                throw e
            }
            catch (e: Exception) {
                if (e.connectCode == Code.UNAUTHENTICATED) tokens?.invalidate()
                failures++
                if (failures > maxFailures) {
                    log.error("[FLYTE] watch of {} failed {} times in a row, giving up", parent.name, maxFailures, e)
                    failure = e
                    synced.completeExceptionally(e)
                    return
                }
                log.warn("[FLYTE] watch of {} failed ({}), reconnecting (attempt {}/{})", parent.name, e.message ?: e.javaClass.simpleName, failures, maxFailures)
            }
            delay(minOf(minBackoffMillis shl minOf(failures - 1, 20), maxBackoffMillis))
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
            stream.receiveClose()
        }
    }

    internal fun observe(update: ActionUpdate) {
        val next = ActionState.of(update)
        var changed = false
        states.compute(next.name) { _, prev ->
            when {
                // a terminal phase is final: ignore a stale update replayed after it
                prev != null && prev.isTerminal && !next.isTerminal -> prev
                prev != null && prev.phase == next.phase && prev.outputUri == next.outputUri -> prev
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
