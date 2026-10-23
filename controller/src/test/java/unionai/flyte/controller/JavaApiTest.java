package unionai.flyte.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import flyteidl2.actions.ActionsServiceOuterClass.AbortRequest;
import flyteidl2.actions.ActionsServiceOuterClass.AbortResponse;
import flyteidl2.actions.ActionsServiceOuterClass.Action;
import flyteidl2.actions.ActionsServiceOuterClass.EnqueueRequest;
import flyteidl2.actions.ActionsServiceOuterClass.EnqueueResponse;
import flyteidl2.actions.ActionsServiceOuterClass.WatchForUpdatesResponse;
import flyteidl2.auth.AuthService.GetPublicClientConfigResponse;
import flyteidl2.common.Identifier.ActionIdentifier;
import flyteidl2.common.Identifier.RunIdentifier;
import flyteidl2.common.Phase.ActionPhase;
import flyteidl2.workflow.StateServiceOuterClass.ActionUpdate;
import flyteidl2.workflow.StateServiceOuterClass.ControlMessage;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import mockwebserver3.Dispatcher;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import okio.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Uses the controller as a Java program would, against a fake Flyte speaking the Connect protocol.
 */
class JavaApiTest {

    private static final RunIdentifier RUN = RunIdentifier.newBuilder()
        .setOrg("acme").setProject("proj").setDomain("development").setName("r1").build();

    private final MockWebServer server = new MockWebServer();
    private final FakeFlyte flyte = new FakeFlyte();

    @BeforeEach
    void start() throws Exception {
        server.setDispatcher(flyte);
        server.start();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private FlyteConnection connect() {
        return FlyteConnection.builder(server.url("/").toString())
            .tokenSource(() -> "test-token")
            .maxQps(50)
            .build();
    }

    private static Action action(String name) {
        return Action.newBuilder()
            .setActionId(ActionIdentifier.newBuilder().setRun(RUN).setName(name))
            .setParentActionName("a0")
            .build();
    }

    @Test
    void submitsAndWatchesActions() throws Exception {
        BlockingQueue<ActionState> seen = new LinkedBlockingQueue<>();
        try (FlyteConnection connection = connect();
             ActionController controller = connection.controller(RUN, "a0")) {
            controller.addListener(seen::add);
            controller.start(Duration.ofSeconds(10));
            assertNull(controller.state("nf-a"));

            assertEquals(Submitted.CREATED, controller.submit(action("nf-a")).get(10, TimeUnit.SECONDS));
            assertEquals(Submitted.EXISTS, controller.submit(action("nf-a")).get(10, TimeUnit.SECONDS));

            // the fake reports enqueued actions as succeeded on its next watch
            ActionState state = seen.poll(10, TimeUnit.SECONDS);
            assertEquals("nf-a", state.getName());
            assertEquals(ActionPhase.ACTION_PHASE_SUCCEEDED, state.getPhase());
            assertTrue(state.isSucceeded() && state.isTerminal());
            assertEquals("s3://bucket/nf-a", controller.state("nf-a").getOutputUri());

            controller.abort("nf-b", "not needed").get(10, TimeUnit.SECONDS);
            assertEquals("not needed", flyte.aborted.get(0).getReason());
        }
        for (RecordedRequest request : flyte.requests) {
            if (request.getUrl().encodedPath().startsWith("/flyteidl2.actions."))
                assertEquals("Bearer test-token", request.getHeaders().get("flyte-authorization"));
        }
    }

    @Test
    void rejectsAnActionOfAnotherParent() {
        try (FlyteConnection connection = connect();
             ActionController controller = connection.controller(RUN, "a0")) {
            Action other = action("nf-a").toBuilder().setParentActionName("a1").build();
            assertThrows(IllegalArgumentException.class, () -> controller.submit(other));
        }
    }

    @Test
    void needsAWayToAuthenticate() {
        assertThrows(FlyteException.class, () -> FlyteConnection.builder("flyte.example.com").build());
        assertThrows(FlyteException.class, () -> FlyteConnection.builder("flyte.example.com").apiKey("not-a-key"));
    }

    /** Flyte's auth metadata and Actions services, enough for one controller */
    private static class FakeFlyte extends Dispatcher {
        final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        final List<AbortRequest> aborted = new CopyOnWriteArrayList<>();
        final ConcurrentHashMap<String, Boolean> enqueued = new ConcurrentHashMap<>();

        @Override
        public MockResponse dispatch(RecordedRequest request) {
            requests.add(request);
            try {
                switch (request.getUrl().encodedPath()) {
                    case "/flyteidl2.auth.AuthMetadataService/GetPublicClientConfig":
                        return unary(GetPublicClientConfigResponse.newBuilder().setAuthorizationMetadataKey("flyte-authorization").build().toByteArray());
                    case "/flyteidl2.actions.ActionsService/Enqueue": {
                        String name = EnqueueRequest.parseFrom(request.getBody().toByteArray()).getAction().getActionId().getName();
                        if (enqueued.putIfAbsent(name, true) != null)
                            return error(409, "already_exists");
                        return unary(EnqueueResponse.getDefaultInstance().toByteArray());
                    }
                    case "/flyteidl2.actions.ActionsService/Abort":
                        aborted.add(AbortRequest.parseFrom(request.getBody().toByteArray()));
                        return unary(AbortResponse.getDefaultInstance().toByteArray());
                    case "/flyteidl2.actions.ActionsService/WatchForUpdates":
                        return watch();
                    default:
                        return error(404, "unimplemented");
                }
            }
            catch (Exception e) {
                return error(500, "internal");
            }
        }

        /** Lists the enqueued actions as succeeded, then ends: the controller watches again */
        private MockResponse watch() {
            Buffer body = new Buffer();
            for (String name : enqueued.keySet()) {
                frame(body, 0, WatchForUpdatesResponse.newBuilder().setActionUpdate(ActionUpdate.newBuilder()
                    .setActionId(ActionIdentifier.newBuilder().setRun(RUN).setName(name))
                    .setPhase(ActionPhase.ACTION_PHASE_SUCCEEDED)
                    .setOutputUri("s3://bucket/" + name)).build().toByteArray());
            }
            frame(body, 0, WatchForUpdatesResponse.newBuilder().setControlMessage(ControlMessage.newBuilder().setSentinel(true)).build().toByteArray());
            frame(body, 2, "{}".getBytes(StandardCharsets.UTF_8));
            return new MockResponse.Builder().code(200).addHeader("content-type", "application/connect+proto").body(body).build();
        }

        /** A Connect stream message: flags, length, payload */
        private static void frame(Buffer body, int flags, byte[] payload) {
            body.writeByte(flags).writeInt(payload.length).write(payload);
        }

        private static MockResponse unary(byte[] message) {
            return new MockResponse.Builder().code(200).addHeader("content-type", "application/proto").body(new Buffer().write(message)).build();
        }

        private static MockResponse error(int status, String code) {
            return new MockResponse.Builder().code(status).addHeader("content-type", "application/json")
                .body("{\"code\":\"" + code + "\",\"message\":\"fake\"}").build();
        }
    }
}
