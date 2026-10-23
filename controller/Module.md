# Module Flyte controller

Runs Flyte v2 actions from any JVM language. It enqueues them through the Actions service and
learns their state from the service's watch, so reading a state never calls Flyte.

# Package unionai.flyte.controller

The controller's public API. Every type here can be used from Java, Kotlin, Groovy or Scala.
Messages (`Action`, `RunIdentifier`, `ActionPhase`, ...) are the protobuf-lite classes generated
from flyteidl2, in the `flyteidl2.*` packages.

The API has three parts:

- **[FlyteConnection]** is the connection to a Flyte control plane, built with
  `FlyteConnection.builder(endpoint)`. Open one per process and share it.
- **[ActionController]** runs the child actions of one parent action. It enqueues them with
  [ActionController.submit], reports their state with [ActionController.state] and
  [ActionListener], and aborts them with [ActionController.abort].
- **[RunClient]** makes the blocking Run service calls: creating and aborting runs, and reading
  an action's details and data.

Behaviour you can rely on:

- **Watch, don't poll.** The controller opens one `WatchForUpdates` stream per parent. It first
  lists the existing children, then streams each change. After a disconnect it reconnects,
  receives the full list again, and catches up. It also reopens the stream every 5 minutes, so a
  stream that stalls silently can't hide updates for long.
- **Finished means final.** Once an action is terminal, its state never moves back, even if a
  stale update is replayed.
- **Names are idempotent.** Submitting an existing name returns [Submitted.EXISTS] and leaves
  that action alone. Resubmitting after a crash is therefore safe.
- **Retries.** Transient failures are retried with exponential backoff: for about 20 minutes on
  submit, and for about 15 seconds on calls a caller waits on. After that, the call fails with
  [FlyteException]. If the watch can't reconnect for about 20 minutes, it gives up and calls
  [ActionListener.onWatchFailed], and [ActionController.state] throws from then on.
- **Threads.** Every type is thread-safe. Futures and listeners run on the controller's threads,
  so keep that work short or move it to your own executor.
