package unionai.flyte.controller

import flyteidl2.common.Identifier.ActionIdentifier
import flyteidl2.common.Identifier.RunIdentifier
import flyteidl2.common.Run.OffloadedInputData
import flyteidl2.dataproxy.DataproxyService.UploadInputsRequest
import flyteidl2.workflow.RunDefinition.ActionDetails
import flyteidl2.workflow.RunDefinition.Run
import flyteidl2.workflow.RunServiceOuterClass.CreateRunRequest
import flyteidl2.workflow.RunServiceOuterClass.GetActionDataResponse
import flyteidl2.workflow.RunServiceOuterClass.GetActionDataURIsResponse

/**
 * The Run service calls an [ActionController] doesn't cover: creating and aborting a run, and
 * reading an action's details and data. Get it from [FlyteConnection.runs].
 *
 * The calls block the calling thread, retry transient failures for about 15 seconds, and then
 * throw [FlyteException]. Thread-safe.
 */
public interface RunClient {

    /**
     * Store a run's inputs (`DataProxyService/UploadInputs`), for [createRun] to reference. Call
     * it even when the run has no inputs: Flyte's UI reads them from there.
     */
    public fun uploadInputs(request: UploadInputsRequest): OffloadedInputData

    /**
     * Create a run under the request's run id.
     *
     * @return the run, or null when it already exists, e.g. when a retried call's first
     *      attempt created it
     */
    public fun createRun(request: CreateRunRequest): Run?

    /** Abort the run: it and all its actions end ABORTED */
    public fun abortRun(runId: RunIdentifier, reason: String)

    /** Where the action's inputs and outputs are stored */
    public fun getActionDataUris(actionId: ActionIdentifier): GetActionDataURIsResponse

    /** @return the action's details, or null when the Run service doesn't know it (yet) */
    public fun getActionDetails(actionId: ActionIdentifier): ActionDetails?

    /** The action's inputs and outputs */
    public fun getActionData(actionId: ActionIdentifier): GetActionDataResponse
}
