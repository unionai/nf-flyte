package unionai.flyte.controller

import com.connectrpc.Code
import com.connectrpc.Headers
import flyteidl2.actions.ActionsServiceClientInterface
import flyteidl2.actions.ActionsServiceOuterClass.AbortRequest
import flyteidl2.actions.ActionsServiceOuterClass.Action
import flyteidl2.actions.ActionsServiceOuterClass.EnqueueRequest
import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.common.Identifier.RunIdentifier
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.future.asCompletableFuture
import kotlinx.coroutines.future.future
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory

/**
 * Runs actions under one parent action: enqueues them through the Actions service and knows their
 * state from its watch (see [Informer]), so reading it never calls Flyte.
 */
interface ActionController : AutoCloseable {

    val runId: RunIdentifier

    val parentAction: String

    /**
     * Start watching the parent's children and wait until the watch has listed the existing ones,
     * so [state] knows about actions created before this controller.
     */
    fun start(timeout: Duration)

    /**
     * Enqueue an action under the parent. Transient failures are retried.
     *
     * @return [Submitted.EXISTS] when an action with that name already exists: it isn't replaced,
     *      and [state] reports the existing one
     */
    fun submit(action: Action): CompletableFuture<Submitted>

    /**
     * @return the last state the watch reported for the named action, or null when it hasn't
     *      reported any yet, e.g. just after it was enqueued
     * @throws FlyteException when the watch has failed for good
     */
    fun state(name: String): ActionState?

    /**
     * Abort the named action and its descendants. A missing or finished action is ignored.
     */
    fun abort(name: String, reason: String): CompletableFuture<Void?>

    /** Called for every state change of a child action */
    fun addListener(listener: ActionListener)

    /** Stop watching, after waiting briefly for pending aborts */
    override fun close()
}

enum class Submitted { CREATED, EXISTS }

internal class DefaultActionController(
    private val actions: ActionsServiceClientInterface,
    override val runId: RunIdentifier,
    override val parentAction: String,
    private val tokens: TokenSource?,
    private val caller: Caller = Caller(tokens = tokens),
) : ActionController {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineName("flyte-controller"))
    private val listeners = CopyOnWriteArrayList<ActionListener>()
    private val aborts = CopyOnWriteArrayList<Deferred<Void?>>()

    /** The Actions service routes every call by these headers, and rejects calls without them */
    private val routing: Headers = mapOf(
        "x-actions-project" to listOf(runId.project),
        "x-actions-domain" to listOf(runId.domain),
        "x-actions-run" to listOf(runId.name),
        "x-actions-parent-action" to listOf(parentAction),
    )

    private val informer = Informer(
        actions = actions,
        parent = actionId(parentAction),
        headers = routing,
        tokens = tokens,
        listener = ::notify,
    )
    private var watch: Job? = null

    override fun start(timeout: Duration) {
        check(watch == null) { "controller already started" }
        watch = scope.launch(CoroutineName("flyte-watch-$parentAction")) { informer.run() }
        runBlocking {
            try {
                withTimeout(timeout.toMillis()) { informer.awaitSynced() }
            }
            catch (e: TimeoutCancellationException) {
                throw FlyteException("Timed out after ${timeout.seconds}s waiting for the Flyte watch of run ${runId.name}, action $parentAction", e)
            }
            catch (e: Exception) {
                throw FlyteException("Unable to watch Flyte run ${runId.name}, action $parentAction: ${e.message}", e)
            }
        }
        log.debug("[FLYTE] watching children of {}/{}: {} existing", runId.name, parentAction, informer.size)
    }

    override fun submit(action: Action): CompletableFuture<Submitted> {
        require(action.parentActionName == parentAction) {
            "action ${action.actionId.name} has parent ${action.parentActionName}, expected $parentAction"
        }
        val request = EnqueueRequest.newBuilder().setAction(action).build()
        return scope.future {
            val result = caller.unary("Enqueue ${action.actionId.name}", accept = setOf(Code.ALREADY_EXISTS)) {
                actions.enqueue(request, routing)
            }
            if (result.isSuccess) Submitted.CREATED else Submitted.EXISTS
        }
    }

    override fun state(name: String): ActionState? {
        informer.failure?.let { throw FlyteException("The Flyte watch of run ${runId.name} failed: ${it.message}", it) }
        return informer[name]
    }

    override fun abort(name: String, reason: String): CompletableFuture<Void?> {
        if (informer[name]?.isTerminal == true)
            return CompletableFuture.completedFuture(null)
        val request = AbortRequest.newBuilder().setActionId(actionId(name)).setReason(reason).build()
        val job = scope.async<Void?> {
            // not found, or already terminal
            caller.unary("Abort $name", accept = setOf(Code.NOT_FOUND, Code.FAILED_PRECONDITION)) { actions.abort(request, routing) }
            null
        }
        aborts.add(job)
        job.invokeOnCompletion { aborts.remove(job) }
        return job.asCompletableFuture()
    }

    override fun addListener(listener: ActionListener) {
        listeners.add(listener)
    }

    private fun notify(state: ActionState) {
        for (l in listeners) {
            try {
                l.onUpdate(state)
            }
            catch (e: Exception) {
                log.warn("[FLYTE] action listener failed on {}", state, e)
            }
        }
    }

    override fun close() {
        runBlocking {
            withTimeoutOrNull(ABORT_GRACE.toMillis()) { aborts.toList().joinAll() }
        }
        scope.cancel()
    }

    private fun actionId(name: String): ActionIdentifier =
        ActionIdentifier.newBuilder().setRun(runId).setName(name).build()

    private companion object {
        val log = LoggerFactory.getLogger(DefaultActionController::class.java)
        val ABORT_GRACE: Duration = Duration.ofSeconds(15)
    }
}
