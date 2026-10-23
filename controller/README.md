# Flyte controller for the JVM

Runs Flyte v2 actions from Java, Kotlin, Groovy, Scala or any other JVM language. Actions are
enqueued through Flyte's Actions service. Their state comes from the service's `WatchForUpdates`
stream and is kept in memory (an informer), so callers never poll Flyte. It is the JVM
counterpart of the Flyte SDK's remote controller, and the nf-flyte Nextflow plugin is built on
it.

- Java 17 or later.
- Talks to the control plane with [connect-kotlin](https://github.com/connectrpc/connect-kotlin)
  over OkHttp.
- Messages are the protobuf-lite classes generated from
  [flyteidl2](https://github.com/flyteorg/flyte/tree/v2/flyteidl2), at the version in
  `src/main/proto/FLYTE_REF`.
- Logs through SLF4J, which you provide.

## Install

```sh
./gradlew :controller:publishToMavenLocal
```

```groovy
dependencies {
    implementation 'ai.union.flyte:flyte-controller:0.1.0'
    implementation 'org.slf4j:slf4j-simple:2.0.17'   // or any SLF4J binding
}
```

## Use

The API is a handful of types in `unionai.flyte.controller`. They are documented in the KDoc
(`./gradlew :controller:dokkaGeneratePublicationJavadoc` writes the Javadoc to
`build/dokka/javadoc`).

| Type | What it does |
| --- | --- |
| `FlyteConnection` | Connection to a control plane, made with `FlyteConnection.builder(endpoint)`. Share one per process. |
| `ActionController` | Submits, watches and aborts the child actions of one parent action. |
| `ActionState`, `ActionListener` | An action's last known state, and a callback for every change. |
| `RunClient` | Blocking Run service calls: create or abort a run, read an action's details and data. |
| `TokenSource`, `ApiKey` | Authentication. |
| `FlyteException` | A call failed for good. |

The typical flow is:

1. Build a connection and authenticate.
2. Get a controller for the parent action, usually the run's root `a0`, and add listeners.
3. `start()` the controller, which waits until the watch has listed the existing children.
4. `submit()` actions, then follow them through the listener or `state()`.

### Java

```java
import flyteidl2.actions.ActionsServiceOuterClass.Action;
import flyteidl2.common.Identifier.ActionIdentifier;
import flyteidl2.common.Identifier.RunIdentifier;
import unionai.flyte.controller.*;

RunIdentifier run = RunIdentifier.newBuilder()
    .setOrg("acme").setProject("flytesnacks").setDomain("development").setName("r1").build();

try (FlyteConnection connection = FlyteConnection.builder("tenant.hosted.unionai.cloud")
         .apiKey(System.getenv("FLYTE_API_KEY"))
         .build();
     ActionController controller = connection.controller(run, "a0")) {

    CompletableFuture<ActionState> done = new CompletableFuture<>();
    controller.addListener(state -> {
        if (state.getName().equals("step-1") && state.isTerminal()) done.complete(state);
    });
    controller.start(Duration.ofMinutes(2));

    Action action = Action.newBuilder()
        .setActionId(ActionIdentifier.newBuilder().setRun(run).setName("step-1"))
        .setParentActionName("a0")
        .setInputUri("s3://bucket/run/step-1/inputs.pb")
        .setRunOutputBase("s3://bucket/run")
        .setTask(taskAction)                       // a flyteidl2.workflow.TaskAction
        .build();
    controller.submit(action).join();              // CREATED, or EXISTS if submitted before

    ActionState state = done.get();
    if (!state.isSucceeded()) throw new IllegalStateException(state.getError().getMessage());
}
```

### Kotlin

The futures are `CompletableFuture`s, so in coroutines you can `await()` them (from
`kotlinx.coroutines.future`):

```kotlin
FlyteConnection.builder("tenant.hosted.unionai.cloud").authCommand("flyte get-token").build().use { connection ->
    connection.controller(run, "a0").use { controller ->
        controller.addListener { state -> println("${state.name}: ${state.phase}") }
        controller.start(Duration.ofMinutes(2))
        val result: Submitted = controller.submit(action).await()
    }
}
```

### Groovy

```groovy
def connection = FlyteConnection.builder(endpoint).apiKey(apiKey).build()
def controller = connection.controller(run, 'a0')
controller.addListener { ActionState state -> if( state.started ) monitor.signal() }
controller.start(Duration.ofMinutes(2))
```

To be told when the watch gives up, implement `onWatchFailed` too:
`new ActionListener() { void onUpdate(ActionState s) {...}; void onWatchFailed(Throwable e) {...} }`.
The plugin's `FlyteExecutor` does this.

### Scala

```scala
val connection = FlyteConnection.builder("tenant.hosted.unionai.cloud").tokenSource(() => sys.env("FLYTE_TOKEN")).build()
val controller = connection.controller(run, "a0")
controller.addListener(state => if (state.isTerminal) println(state))
controller.start(java.time.Duration.ofMinutes(2))
```

## Authentication

Set exactly one of the following on the builder:

- **`apiKey(key)`:** a Flyte API key, exchanged for access tokens with the OAuth2
  client-credentials grant, as the Flyte SDK does.
- **`authCommand(cmd)`:** a shell command whose last line of output is an access token. It runs
  again every 5 minutes, or when the server rejects the token.
- **`tokenSource(source)`:** your own `TokenSource`, which must be thread-safe and should cache
  its token. Implement `invalidate` so a rejected token gets replaced.

Tokens are sent in the header that the server's public client config names, for example
`flyte-authorization`. A call rejected with HTTP 401 is retried with a new token.

## Guarantees

The guarantees on the watch, on finished states, on idempotent names, on retries and on threads
are listed in [Module.md](Module.md). In short:

- A state you read is at most one reconnect behind.
- Once an action is terminal, it stays terminal.
- Resubmitting a name never runs the action twice.

## Develop

- **Run the tests:** `./gradlew :controller:test`. There are unit tests of the informer, the
  controller and auth, and `JavaApiTest`, which drives the public API from Java against a fake
  Connect server.
- **Public API:** the API is pinned in [`api/controller.api`](api/controller.api), and
  `./gradlew check` fails when the API changes without it. After an intended change, run
  `./gradlew :controller:updateKotlinAbi` and commit the new file. The build uses
  [explicit API mode](https://kotlinlang.org/docs/whatsnew14.html#explicit-api-mode-for-library-authors),
  so everything public is declared `public` on purpose.
- **Implementation:** the implementation is in `unionai.flyte.controller.internal`. It is not
  part of the API and may change at any time.
- **Protos:** the flyteidl2 protos are vendored in `src/main/proto`. Refresh them with
  `scripts/update-protos.sh [flyte-ref]`.
