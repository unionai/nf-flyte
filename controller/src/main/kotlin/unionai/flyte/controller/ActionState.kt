package unionai.flyte.controller

import flyteidl2.common.Phase.ActionPhase
import flyteidl2.core.Execution.ExecutionError
import flyteidl2.workflow.StateServiceOuterClass.ActionUpdate

/**
 * A Flyte call failed for good: a permanent error, or a transient one that outlasted its retries.
 * The cause is the last error, usually a `com.connectrpc.ConnectException` carrying its code.
 */
public open class FlyteException @JvmOverloads constructor(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * The last state the watch reported for an action. Immutable.
 */
public class ActionState(
    /** The action's name, unique within its run */
    public val name: String,
    public val phase: ActionPhase,
    /** Why the action failed, if it did and the service said so */
    public val error: ExecutionError?,
    /** Prefix of the action's outputs (`<outputUri>/outputs.pb`), or empty until it has any */
    public val outputUri: String,
) {
    /** Finished, one way or another: the phase won't change again */
    public val isTerminal: Boolean get() = phase in TERMINAL

    /** Succeeded, or recovered from a previous run, which is as good */
    public val isSucceeded: Boolean get() = phase == ActionPhase.ACTION_PHASE_SUCCEEDED || phase == ActionPhase.ACTION_PHASE_RECOVERED

    /** Running on a worker, or done */
    public val isStarted: Boolean get() = phase == ActionPhase.ACTION_PHASE_RUNNING || isTerminal

    override fun equals(other: Any?): Boolean =
        other is ActionState && name == other.name && phase == other.phase && error == other.error && outputUri == other.outputUri

    override fun hashCode(): Int = name.hashCode() * 31 + phase.hashCode()

    override fun toString(): String = "ActionState(name=$name, phase=$phase, outputUri=$outputUri)"

    public companion object {
        /** The phases an action ends in */
        @JvmField
        public val TERMINAL: Set<ActionPhase> = setOf(
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
 * Notified of what an [ActionController]'s watch sees. Called on the controller's threads, one
 * update at a time per controller: keep it short, and hand any slow work to another thread.
 *
 * A lambda implements [onUpdate]; implement [onWatchFailed] too to learn that the watch gave up.
 */
public fun interface ActionListener {

    /** A child action was seen for the first time, or its phase, outputs or error changed */
    public fun onUpdate(state: ActionState)

    /**
     * The watch gave up after failing to reconnect for about 20 minutes: no more updates will
     * come, and [ActionController.state] now throws.
     */
    public fun onWatchFailed(error: Throwable) {}
}
