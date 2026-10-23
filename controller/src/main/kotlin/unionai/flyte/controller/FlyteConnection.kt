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

/**
 * A connection to a Flyte v2 control plane, speaking Connect with binary protobuf over OkHttp.
 *
 * Every call carries the bearer token from [TokenSource]; the header it goes in comes from the
 * server's public client config (e.g. `flyte-authorization`).
 */
class FlyteConnection private constructor(
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

    /** Run service calls, for creating and aborting runs */
    val runs: RunClient = DefaultRunClient(RunServiceClient(protocol), DataProxyServiceClient(protocol), Caller(Retry.blocking()))

    /**
     * A controller for the child actions of [parentAction] in the given run. Call
     * [ActionController.start] before using it.
     */
    fun controller(runId: RunIdentifier, parentAction: String): ActionController =
        DefaultActionController(actions, runId, parentAction, tokens, Caller(Retry.actions(), limiter), Caller(Retry.blocking(), limiter))

    override fun close() {
        // ends the calls still open, e.g. a watch whose controller wasn't closed
        http.dispatcher.cancelAll()
        http.dispatcher.executorService.shutdown()
        http.connectionPool.evictAll()
    }

    companion object {
        private const val UNARY_TIMEOUT_SECONDS = 15L

        /**
         * @param endpoint host of the Flyte API, e.g. `tenant.hosted.unionai.cloud`, or a URL
         * @param apiKey Flyte API key, used when there is no [authCommand]
         * @param authCommand command that prints an access token
         * @param maxQps most enqueue / abort / run calls a second
         */
        @JvmStatic
        @JvmOverloads
        fun create(endpoint: String, apiKey: String?, authCommand: String?, maxQps: Int = 100): FlyteConnection {
            val baseUrl = (if ("://" in endpoint) endpoint else "https://$endpoint").trimEnd('/')
            return FlyteConnection(baseUrl, { metadata, http ->
                if (!authCommand.isNullOrBlank()) CommandTokenSource(authCommand)
                else ClientCredentialsTokenSource(ApiKey.decode(apiKey), metadata, http)
            }, maxQps)
        }
    }
}
