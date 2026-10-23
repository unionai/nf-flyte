package unionai.flyte.controller

import flyteidl2.actions.ActionsServiceOuterClass.Action
import flyteidl2.common.Identifier.RunIdentifier
import java.time.Duration
import java.util.concurrent.CompletableFuture

/**
 * Runs the child actions of one parent action: enqueues them through the Actions service, and
 * keeps their state in memory from the service's watch, so reading it never calls Flyte.
 *
 * Get one from [FlyteConnection.controller], add listeners, then [start] it:
 * ```java
 * try (ActionController controller = connection.controller(runId, "a0")) {
 *     controller.addListener(state -> { if (state.isTerminal()) done.countDown(); });
 *     controller.start(Duration.ofMinutes(2));
 *     controller.submit(action).join();
 *     // ... wait for the listener, then
 *     ActionState state = controller.state("my-action");
 * }
 * ```
 *
 * All methods are thread-safe. The futures complete on the controller's threads: chain slow work
 * with an `...Async` variant and your own executor.
 */
public interface ActionController : AutoCloseable {

    /** The run the actions belong to */
    public val runId: RunIdentifier

    /** Name of the parent action, e.g. `a0` for the run's root */
    public val parentAction: String

    /**
     * Start watching the parent's children, and wait until the watch has listed those that
     * already exist, so that [state] also knows about actions created before this controller,
     * e.g. by a previous attempt of the parent. Call it once.
     *
     * @throws FlyteException when the watch didn't list them within [timeout]
     */
    public fun start(timeout: Duration)

    /**
     * Enqueue an action under the parent. Transient failures are retried for about 20 minutes,
     * so a control plane restart doesn't fail the action. Its state then arrives through the
     * watch, starting with [state] returning null until the service reports it.
     *
     * Names are the idempotency key: submitting a name again is safe, and doesn't replace or
     * rerun the existing action. To rerun one, use a new name.
     *
     * @param action an action whose `parent_action_name` is [parentAction]
     * @return a future of [Submitted.CREATED], or [Submitted.EXISTS] when an action with that
     *      name already existed; it fails with [FlyteException] on a permanent error
     */
    public fun submit(action: Action): CompletableFuture<Submitted>

    /**
     * @param name the action's name, i.e. `action_id.name`
     * @return the last state the watch reported for the action, or null when it reported none,
     *      e.g. just after it was submitted, or before [start]
     * @throws FlyteException when the watch has failed for good (see [ActionListener.onWatchFailed])
     */
    public fun state(name: String): ActionState?

    /**
     * Abort the named action, and with it its descendants. An action that is missing or already
     * finished is left alone. Gives up after about 15 seconds of retries.
     *
     * @return a future completing once the service accepted the abort; the action's ABORTED
     *      phase then arrives through the watch
     */
    public fun abort(name: String, reason: String): CompletableFuture<Void?>

    /**
     * Notify [listener] of every change the watch reports. Add listeners before [start] to see
     * the actions it lists first.
     */
    public fun addListener(listener: ActionListener)

    public fun removeListener(listener: ActionListener)

    /**
     * Stop watching, after waiting up to 15 seconds for pending aborts. The actions keep
     * running in Flyte. Doesn't close the [FlyteConnection].
     */
    override fun close()
}

/** What [ActionController.submit] did */
public enum class Submitted {
    /** Enqueued a new action */
    CREATED,

    /** An action with that name already existed: it was left as it is */
    EXISTS,
}
