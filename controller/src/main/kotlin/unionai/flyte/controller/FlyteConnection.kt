package unionai.flyte.controller

import com.connectrpc.ProtocolClientConfig
import com.connectrpc.extensions.GoogleJavaLiteProtobufStrategy
import com.connectrpc.impl.ProtocolClient
import com.connectrpc.okhttp.ConnectOkHttpClient
import com.connectrpc.protocols.NetworkProtocol
import com.connectrpc.simpleTimeouts
import flyteidl2.actions.ActionsServiceClient
import flyteidl2.common.Identifier.RunIdentifier
import flyteidl2.dataproxy.DataProxyServiceClient
import flyteidl2.workflow.RunServiceClient
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import unionai.flyte.controller.internal.AuthInterceptor
import unionai.flyte.controller.internal.AuthMetadata
import unionai.flyte.controller.internal.Caller
import unionai.flyte.controller.internal.ClientCredentialsTokenSource
import unionai.flyte.controller.internal.CommandTokenSource
import unionai.flyte.controller.internal.DefaultActionController
import unionai.flyte.controller.internal.DefaultRunClient
import unionai.flyte.controller.internal.RateLimiter
import unionai.flyte.controller.internal.Retry

/**
 * A connection to a Flyte v2 control plane, speaking Connect with binary protobuf over OkHttp.
 * Open one per process and share it: its controllers and [runs] share its connections, token
 * and rate limit.
 *
 * ```java
 * try (FlyteConnection connection = FlyteConnection.builder("tenant.hosted.unionai.cloud")
 *         .apiKey(System.getenv("FLYTE_API_KEY"))
 *         .build()) {
 *     ...
 * }
 * ```
 *
 * Every call carries a bearer token, in the header the server's public client config names
 * (e.g. `flyte-authorization`). A call rejected with HTTP 401 is retried with a new token.
 */
public class FlyteConnection private constructor(
    baseUrl: String,
    makeTokens: (AuthMetadata, OkHttpClient) -> TokenSource,
    maxQps: Int,
) : AutoCloseable {

    private val http: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        // detects a dead connection, which the long-lived watch would otherwise wait on forever
        .pingInterval(30, TimeUnit.SECONDS)
        .dispatcher(Dispatcher().apply {
            maxRequests = 512
            maxRequestsPerHost = 512
        })
        .build()

    private val metadata = AuthMetadata(baseUrl, http)
    private val tokens: TokenSource = makeTokens(metadata, http)
    private val limiter = RateLimiter(maxQps)

    private val protocol: ProtocolClient = run {
        // connect-kotlin's settings (no read timeouts, no silent retry of a 408) for both clients
        fun connect() = ConnectOkHttpClient.configureClient(
            http.newBuilder().addInterceptor(AuthInterceptor(tokens) { metadata.authorizationHeader() })
        )
        ProtocolClient(
            ConnectOkHttpClient(connect().build(), connect().build()),
            ProtocolClientConfig(
                host = baseUrl,
                serializationStrategy = GoogleJavaLiteProtobufStrategy(),
                networkProtocol = NetworkProtocol.CONNECT,
                ioCoroutineContext = Dispatchers.IO,
                // unary calls time out; the watch stays open for as long as the pipeline runs
                timeoutOracle = simpleTimeouts(UNARY_TIMEOUT_SECONDS.seconds, null),
            ),
        )
    }

    private val actions = ActionsServiceClient(protocol)

    /** Run service calls, for creating and aborting runs, and reading actions' data */
    public val runs: RunClient = DefaultRunClient(RunServiceClient(protocol), DataProxyServiceClient(protocol), Caller(Retry.blocking(), limiter))

    /**
     * A controller for the child actions of [parentAction] in the given run, e.g. of `a0`, the
     * run's root action. Call [ActionController.start] before using it, and close it when done.
     */
    public fun controller(runId: RunIdentifier, parentAction: String): ActionController =
        DefaultActionController(actions, runId, parentAction, tokens, Caller(Retry.actions(), limiter), Caller(Retry.blocking(), limiter))

    /**
     * Close the connection, ending the watch of any controller still open. Close controllers
     * first, so they can wait for their pending aborts.
     */
    override fun close() {
        // ends the calls still open, e.g. a watch whose controller wasn't closed
        http.dispatcher.cancelAll()
        http.dispatcher.executorService.shutdown()
        http.connectionPool.evictAll()
    }

    /**
     * Configures a [FlyteConnection]. Set one way to authenticate: [apiKey], [authCommand] or
     * [tokenSource]; the last one set wins.
     */
    public class Builder internal constructor(endpoint: String) {

        private val baseUrl = (if ("://" in endpoint) endpoint else "https://$endpoint").trimEnd('/')
        private var tokens: ((AuthMetadata, OkHttpClient) -> TokenSource)? = null
        private var maxQps = 100

        /**
         * Authenticate with a Flyte API key (see [ApiKey]), exchanged for access tokens with the
         * OAuth2 client-credentials grant, as the Flyte SDK does.
         *
         * @throws FlyteException when the key is malformed
         */
        public fun apiKey(key: String): Builder = apply {
            val decoded = ApiKey.decode(key)
            tokens = { metadata, http -> ClientCredentialsTokenSource(decoded, metadata, http) }
        }

        /**
         * Authenticate with the access token a shell command prints, as its last line of output,
         * e.g. a CLI that logs you in. It's run again after 5 minutes, or when the token is
         * rejected, and may take up to 5 minutes.
         */
        public fun authCommand(command: String): Builder = apply {
            require(command.isNotBlank()) { "empty auth command" }
            tokens = { _, _ -> CommandTokenSource(command) }
        }

        /** Authenticate with tokens from [tokens] */
        public fun tokenSource(tokens: TokenSource): Builder = apply {
            this.tokens = { _, _ -> tokens }
        }

        /** Most enqueue, abort and run calls a second, across the connection's controllers; 100 by default */
        public fun maxQps(maxQps: Int): Builder = apply {
            require(maxQps > 0) { "maxQps must be positive: $maxQps" }
            this.maxQps = maxQps
        }

        /**
         * @throws FlyteException when no way to authenticate was set
         */
        public fun build(): FlyteConnection {
            val tokens = tokens ?: throw FlyteException("Set how to authenticate to Flyte: an API key, an auth command or a token source")
            return FlyteConnection(baseUrl, tokens, maxQps)
        }
    }

    public companion object {
        private const val UNARY_TIMEOUT_SECONDS = 15L

        /**
         * @param endpoint host of the Flyte API, e.g. `tenant.hosted.unionai.cloud`, or its URL,
         *      e.g. `http://localhost:8090` for a local cluster
         */
        @JvmStatic
        public fun builder(endpoint: String): Builder = Builder(endpoint)
    }
}
