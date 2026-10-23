package unionai.flyte.controller.internal

import com.connectrpc.Code
import com.connectrpc.Headers
import com.connectrpc.ResponseMessage
import flyteidl2.actions.ActionsServiceClientInterface
import flyteidl2.actions.ActionsServiceOuterClass.AbortRequest
import flyteidl2.actions.ActionsServiceOuterClass.AbortResponse
import flyteidl2.actions.ActionsServiceOuterClass.Action
import flyteidl2.actions.ActionsServiceOuterClass.EnqueueRequest
import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.common.Identifier.RunIdentifier
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
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
import unionai.flyte.controller.ActionController
import unionai.flyte.controller.ActionListener
import unionai.flyte.controller.ActionState
import unionai.flyte.controller.FlyteException
import unionai.flyte.controller.Submitted
import unionai.flyte.controller.TokenSource

internal class DefaultActionController(
    private val actions: ActionsServiceClientInterface,
    override val runId: RunIdentifier,
    override val parentAction: String,
    private val tokens: TokenSource?,
    private val enqueues: Caller = Caller(Retry.actions()),
    private val aborts: Caller = Caller(Retry.blocking()),
    maxWatchFailures: Int = 40,
) : ActionController {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineName("flyte-controller"))
    private val listeners = CopyOnWriteArrayList<ActionListener>()
    private val pendingAborts = CopyOnWriteArrayList<Deferred<Void?>>()

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
        maxFailures = maxWatchFailures,
    )
    private var watch: Job? = null

    @Synchronized
    override fun start(timeout: Duration) {
        check(watch == null) { "controller already started" }
        watch = scope.launch(CoroutineName("flyte-watch-$parentAction")) {
            informer.run()
            informer.failure?.let(::notifyFailed)
        }
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
            val result = enqueues.unary("Enqueue ${action.actionId.name}", accept = setOf(Code.ALREADY_EXISTS)) {
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
            aborts.unary("Abort $name", accept = setOf(Code.NOT_FOUND)) {
                // the service rejects aborting a finished action with FAILED_PRECONDITION, which
                // also means "wrong shard": retry it until the watch shows the action finished
                if (informer[name]?.isTerminal == true) ResponseMessage.Success(AbortResponse.getDefaultInstance(), emptyMap(), emptyMap())
                else actions.abort(request, routing)
            }
            null
        }
        pendingAborts.add(job)
        job.invokeOnCompletion { pendingAborts.remove(job) }
        return job.asCompletableFuture()
    }

    override fun addListener(listener: ActionListener) {
        listeners.add(listener)
    }

    override fun removeListener(listener: ActionListener) {
        listeners.remove(listener)
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

    private fun notifyFailed(error: Throwable) {
        for (l in listeners) {
            try {
                l.onWatchFailed(error)
            }
            catch (e: Exception) {
                log.warn("[FLYTE] action listener failed on the watch failure", e)
            }
        }
    }

    override fun close() {
        runBlocking {
            withTimeoutOrNull(ABORT_GRACE.toMillis()) { pendingAborts.toList().joinAll() }
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
