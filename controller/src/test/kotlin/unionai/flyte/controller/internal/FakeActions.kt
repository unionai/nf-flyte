package unionai.flyte.controller.internal

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.Headers
import com.connectrpc.ResponseMessage
import com.connectrpc.ServerOnlyStreamInterface
import flyteidl2.actions.ActionsServiceClientInterface
import flyteidl2.actions.ActionsServiceOuterClass.AbortRequest
import flyteidl2.actions.ActionsServiceOuterClass.AbortResponse
import flyteidl2.actions.ActionsServiceOuterClass.EnqueueRequest
import flyteidl2.actions.ActionsServiceOuterClass.EnqueueResponse
import flyteidl2.actions.ActionsServiceOuterClass.SignalRequest
import flyteidl2.actions.ActionsServiceOuterClass.SignalResponse
import flyteidl2.actions.ActionsServiceOuterClass.UpdateRequest
import flyteidl2.actions.ActionsServiceOuterClass.UpdateResponse
import flyteidl2.actions.ActionsServiceOuterClass.WatchForUpdatesRequest
import flyteidl2.actions.ActionsServiceOuterClass.WatchForUpdatesResponse
import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.common.Identifier.RunIdentifier
import flyteidl2.common.Phase.ActionPhase
import flyteidl2.workflow.StateServiceOuterClass.ActionUpdate
import flyteidl2.workflow.StateServiceOuterClass.ControlMessage
import java.util.concurrent.ConcurrentLinkedQueue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel


val RUN: RunIdentifier = RunIdentifier.newBuilder().setOrg("acme").setProject("proj").setDomain("development").setName("r1").build()

fun update(name: String, phase: ActionPhase, outputUri: String = ""): WatchForUpdatesResponse =
    WatchForUpdatesResponse.newBuilder().setActionUpdate(
        ActionUpdate.newBuilder()
            .setActionId(ActionIdentifier.newBuilder().setRun(RUN).setName(name))
            .setPhase(phase)
            .setOutputUri(outputUri)
    ).build()

val SENTINEL: WatchForUpdatesResponse =
    WatchForUpdatesResponse.newBuilder().setControlMessage(ControlMessage.newBuilder().setSentinel(true)).build()

/**
 * Actions service whose watch streams and call results the test scripts.
 */
class FakeActions : ActionsServiceClientInterface {

    /** One channel per watch call, in order: the test sends the stream's messages into it */
    val watches = Channel<Channel<WatchForUpdatesResponse>>(Channel.UNLIMITED)
    val watchRequests = ConcurrentLinkedQueue<Pair<WatchForUpdatesRequest, Headers>>()

    /** Result codes of the next enqueue calls; once empty, enqueues succeed */
    val enqueueResults = ConcurrentLinkedQueue<Code>()
    val enqueued = ConcurrentLinkedQueue<Pair<EnqueueRequest, Headers>>()

    val abortResults = ConcurrentLinkedQueue<Code>()
    val aborted = ConcurrentLinkedQueue<AbortRequest>()

    /** Start a new watch stream for the next watch call */
    fun newWatch(): Channel<WatchForUpdatesResponse> = Channel<WatchForUpdatesResponse>(Channel.UNLIMITED).also { watches.trySend(it) }

    override suspend fun enqueue(request: EnqueueRequest, headers: Headers): ResponseMessage<EnqueueResponse> {
        enqueued.add(request to headers)
        return result(enqueueResults.poll(), EnqueueResponse.getDefaultInstance())
    }

    override suspend fun abort(request: AbortRequest, headers: Headers): ResponseMessage<AbortResponse> {
        aborted.add(request)
        return result(abortResults.poll(), AbortResponse.getDefaultInstance())
    }

    override suspend fun watchForUpdates(headers: Headers): ServerOnlyStreamInterface<WatchForUpdatesRequest, WatchForUpdatesResponse> {
        val channel = watches.receive()
        return object : ServerOnlyStreamInterface<WatchForUpdatesRequest, WatchForUpdatesResponse> {
            override fun responseChannel(): ReceiveChannel<WatchForUpdatesResponse> = channel
            override fun responseHeaders(): Deferred<Headers> = CompletableDeferred(emptyMap())
            override fun responseTrailers(): Deferred<Headers> = CompletableDeferred(emptyMap())
            override suspend fun sendAndClose(input: WatchForUpdatesRequest): Result<Unit> {
                watchRequests.add(input to headers)
                return Result.success(Unit)
            }
            override suspend fun receiveClose() {}
            override fun isReceiveClosed() = channel.isClosedForReceive
            override fun isClosed() = channel.isClosedForReceive
        }
    }

    override suspend fun update(request: UpdateRequest, headers: Headers): ResponseMessage<UpdateResponse> = throw UnsupportedOperationException()

    override suspend fun signal(request: SignalRequest, headers: Headers): ResponseMessage<SignalResponse> = throw UnsupportedOperationException()

    private fun <T> result(code: Code?, ok: T): ResponseMessage<T> =
        if (code == null) ResponseMessage.Success(ok, emptyMap(), emptyMap())
        else ResponseMessage.Failure(ConnectException(code, "scripted ${code.codeName}"), emptyMap(), emptyMap())
}
