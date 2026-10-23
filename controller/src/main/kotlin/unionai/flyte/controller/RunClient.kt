package unionai.flyte.controller

import com.connectrpc.Code
import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.common.Identifier.RunIdentifier
import flyteidl2.common.Run.OffloadedInputData
import flyteidl2.dataproxy.DataProxyServiceClientInterface
import flyteidl2.dataproxy.DataproxyService.UploadInputsRequest
import flyteidl2.workflow.RunDefinition.ActionDetails
import flyteidl2.workflow.RunDefinition.Run
import flyteidl2.workflow.RunServiceClientInterface
import flyteidl2.workflow.RunServiceOuterClass.AbortRunRequest
import flyteidl2.workflow.RunServiceOuterClass.CreateRunRequest
import flyteidl2.workflow.RunServiceOuterClass.GetActionDataRequest
import flyteidl2.workflow.RunServiceOuterClass.GetActionDataResponse
import flyteidl2.workflow.RunServiceOuterClass.GetActionDataURIsRequest
import flyteidl2.workflow.RunServiceOuterClass.GetActionDataURIsResponse
import flyteidl2.workflow.RunServiceOuterClass.GetActionDetailsRequest
import kotlinx.coroutines.runBlocking

/**
 * The few Run service calls the controller doesn't cover: creating and aborting a run, and
 * reading an action's details. They block the calling thread.
 */
interface RunClient {

    /** Store a run's inputs (`DataProxyService/UploadInputs`), for [createRun] to reference */
    fun uploadInputs(request: UploadInputsRequest): OffloadedInputData

    fun createRun(request: CreateRunRequest): Run

    /** Abort the run: it and all its actions end ABORTED */
    fun abortRun(runId: RunIdentifier, reason: String)

    /** Where the action's inputs and outputs are stored */
    fun getActionDataUris(actionId: ActionIdentifier): GetActionDataURIsResponse

    /** @return the action's details, or null when the Run service doesn't know it (yet) */
    fun getActionDetails(actionId: ActionIdentifier): ActionDetails?

    /** The action's inputs and outputs */
    fun getActionData(actionId: ActionIdentifier): GetActionDataResponse
}

internal class DefaultRunClient(
    private val runs: RunServiceClientInterface,
    private val dataProxy: DataProxyServiceClientInterface,
    private val caller: Caller,
) : RunClient {

    override fun uploadInputs(request: UploadInputsRequest): OffloadedInputData = runBlocking {
        caller.unary("UploadInputs") { dataProxy.uploadInputs(request) }.getOrThrow().offloadedInputData
    }

    override fun createRun(request: CreateRunRequest): Run = runBlocking {
        caller.unary("CreateRun") { runs.createRun(request) }.getOrThrow().run
    }

    override fun abortRun(runId: RunIdentifier, reason: String) {
        val request = AbortRunRequest.newBuilder().setRunId(runId).setReason(reason).build()
        runBlocking { caller.unary("AbortRun ${runId.name}") { runs.abortRun(request) }.getOrThrow() }
    }

    override fun getActionDataUris(actionId: ActionIdentifier): GetActionDataURIsResponse {
        val request = GetActionDataURIsRequest.newBuilder().setActionId(actionId).build()
        return runBlocking { caller.unary("GetActionDataURIs ${actionId.name}") { runs.getActionDataURIs(request) }.getOrThrow() }
    }

    override fun getActionDetails(actionId: ActionIdentifier): ActionDetails? {
        val request = GetActionDetailsRequest.newBuilder().setActionId(actionId).build()
        return runBlocking {
            caller.unary("GetActionDetails ${actionId.name}", accept = setOf(Code.NOT_FOUND)) {
                runs.getActionDetails(request)
            }.getOrNull()?.details
        }
    }

    override fun getActionData(actionId: ActionIdentifier): GetActionDataResponse {
        val request = GetActionDataRequest.newBuilder().setActionId(actionId).build()
        return runBlocking { caller.unary("GetActionData ${actionId.name}") { runs.getActionData(request) }.getOrThrow() }
    }
}
