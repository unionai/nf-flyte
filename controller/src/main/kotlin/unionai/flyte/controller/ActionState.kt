package unionai.flyte.controller

import flyteidl2.common.Phase.ActionPhase
import flyteidl2.core.Execution.ExecutionError
import flyteidl2.workflow.StateServiceOuterClass.ActionUpdate

open class FlyteException @JvmOverloads constructor(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * The last state the watch reported for an action.
 */
class ActionState(
    val name: String,
    val phase: ActionPhase,
    /** Why the action failed, if it did and the service said so */
    val error: ExecutionError?,
    /** Prefix of the action's outputs (`<outputUri>/outputs.pb`), once it has any */
    val outputUri: String,
) {
    val isTerminal: Boolean get() = phase in TERMINAL

    /** Succeeded, or recovered from a previous run, which is as good */
    val isSucceeded: Boolean get() = phase == ActionPhase.ACTION_PHASE_SUCCEEDED || phase == ActionPhase.ACTION_PHASE_RECOVERED

    /** Running on a worker, or done */
    val isStarted: Boolean get() = phase == ActionPhase.ACTION_PHASE_RUNNING || isTerminal

    override fun toString() = "ActionState(name=$name, phase=$phase, outputUri=$outputUri)"

    companion object {
        @JvmField
        val TERMINAL: Set<ActionPhase> = setOf(
            ActionPhase.ACTION_PHASE_SUCCEEDED,
            ActionPhase.ACTION_PHASE_FAILED,
            ActionPhase.ACTION_PHASE_ABORTED,
            ActionPhase.ACTION_PHASE_TIMED_OUT,
            ActionPhase.ACTION_PHASE_RECOVERED,
        )

        internal fun of(update: ActionUpdate) = ActionState(
            name = update.actionId.name,
            phase = update.phase,
            error = if (update.hasError()) update.error else null,
            outputUri = update.outputUri,
        )
    }
}

/**
 * Called for every state change the watch reports. Runs on the controller's threads: keep it short.
 */
fun interface ActionListener {
    fun onUpdate(state: ActionState)
}
