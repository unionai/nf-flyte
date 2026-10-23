package unionai.flyte.controller

import com.google.protobuf.MessageLite
import flyteidl2.auth.AuthService.GetOAuth2MetadataRequest
import flyteidl2.auth.AuthService.GetOAuth2MetadataResponse
import flyteidl2.auth.AuthService.GetPublicClientConfigRequest
import flyteidl2.auth.AuthService.GetPublicClientConfigResponse
import com.squareup.moshi.Moshi
import java.io.File
import java.io.IOException
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.concurrent.TimeUnit
import okhttp3.FormBody
import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory

/**
 * Supplies the bearer token sent with every Flyte call.
 */
interface TokenSource {
    /** A valid token, fetching a new one when there is none or it expired */
    fun token(): String

    /**
     * Drop the [rejected] token, unless another caller already replaced it: concurrent calls
     * rejected with the same token then fetch a new one only once.
     */
    fun invalidate(rejected: String)
}

/** Holds the current token and when it expires; callers synchronize on the token source. */
internal class CachedToken {
    var token: String? = null
        private set
    private var expiry: Instant = Instant.EPOCH

    fun valid(): String? = token?.takeIf { Instant.now().isBefore(expiry) }

    fun set(token: String, expiry: Instant): String {
        this.token = token
        this.expiry = expiry
        return token
    }

    fun invalidate(rejected: String) {
        if (token == rejected) {
            token = null
            expiry = Instant.EPOCH
        }
    }
}

/**
 * A Flyte API key: base64("endpoint:client_id:client_secret:org"), as issued by the Flyte CLI.
 */
class ApiKey(
    val clientId: String,
    val clientSecret: String,
    val endpoint: String? = null,
    val org: String? = null,
) {
    companion object {
        @JvmStatic
        fun decode(key: String?): ApiKey {
            if (key.isNullOrBlank()) throw FlyteException("Missing Flyte API key")
            // the endpoint may itself contain colons (e.g. dns:///host), so split from the right
            val parts = String(Base64.getDecoder().decode(key.trim()), StandardCharsets.UTF_8).split(':')
            if (parts.size < 4) throw FlyteException("Invalid Flyte API key: expected 'endpoint:client_id:client_secret:org'")
            val org = parts[parts.size - 1]
            return ApiKey(
                clientId = parts[parts.size - 3],
                clientSecret = parts[parts.size - 2],
                endpoint = parts.subList(0, parts.size - 3).joinToString(":"),
                org = org.takeIf { it.isNotEmpty() && it != "None" },
            )
        }
    }
}

/**
 * Calls the anonymous `AuthMetadataService` methods. They run synchronously on the caller's
 * thread, outside OkHttp's dispatcher, because the auth interceptor calls them while
 * dispatcher-run calls wait for a token.
 */
internal class AuthMetadata(private val baseUrl: String, private val http: OkHttpClient) {

    @Volatile private var publicConfig: GetPublicClientConfigResponse? = null

    fun publicClientConfig(): GetPublicClientConfigResponse =
        publicConfig ?: call("GetPublicClientConfig", GetPublicClientConfigRequest.getDefaultInstance()) {
            GetPublicClientConfigResponse.parseFrom(it)
        }.also { publicConfig = it }

    fun oauth2Metadata(): GetOAuth2MetadataResponse =
        call("GetOAuth2Metadata", GetOAuth2MetadataRequest.getDefaultInstance()) { GetOAuth2MetadataResponse.parseFrom(it) }

    /** Name of the header carrying the bearer token, e.g. `flyte-authorization` */
    fun authorizationHeader(): String = publicClientConfig().authorizationMetadataKey.ifEmpty { "authorization" }

    private fun <T> call(method: String, request: MessageLite, parse: (ByteArray) -> T): T {
        val req = Request.Builder()
            .url("$baseUrl/flyteidl2.auth.AuthMetadataService/$method")
            .header("Connect-Protocol-Version", "1")
            .post(request.toByteArray().toRequestBody(PROTO))
            .build()
        http.newCall(req).execute().use { resp ->
            val body = resp.body.bytes()
            if (!resp.isSuccessful) throw FlyteException("AuthMetadataService/$method -> HTTP ${resp.code}: ${String(body)}")
            return parse(body)
        }
    }

    private companion object {
        val PROTO = "application/proto".toMediaType()
    }
}

/**
 * Exchanges an API key for an access token with the OAuth2 client-credentials grant, as the
 * Flyte SDK does.
 */
internal class ClientCredentialsTokenSource(
    private val apiKey: ApiKey,
    private val metadata: AuthMetadata,
    private val http: OkHttpClient,
) : TokenSource {

    private val cached = CachedToken()

    @Synchronized
    override fun token(): String {
        cached.valid()?.let { return it }
        val form = FormBody.Builder()
            .add("grant_type", "client_credentials")
            .add("scope", "all")
        metadata.publicClientConfig().audience.takeIf { it.isNotEmpty() }?.let { form.add("audience", it) }
        val basic = Base64.getEncoder().encodeToString(
            "${enc(apiKey.clientId)}:${enc(apiKey.clientSecret)}".toByteArray(StandardCharsets.UTF_8)
        )
        val req = Request.Builder()
            .url(metadata.oauth2Metadata().tokenEndpoint)
            .header("Authorization", "Basic $basic")
            .post(form.build())
            .build()
        http.newCall(req).execute().use { resp ->
            val body = resp.body.string()
            if (!resp.isSuccessful) throw FlyteException("Unable to get a Flyte access token -> HTTP ${resp.code}: $body")
            val json = JSON.fromJson(body) ?: emptyMap<String, Any?>()
            val expiresIn = (json["expires_in"] as? Number)?.toLong() ?: 3600
            val access = json["access_token"] as? String ?: throw FlyteException("Flyte token endpoint returned no access_token")
            log.debug("[FLYTE] obtained access token, expires in {}s", expiresIn)
            // refresh a minute early
            return cached.set(access, Instant.now().plusSeconds(maxOf(expiresIn - 60, 30)))
        }
    }

    @Synchronized
    override fun invalidate(rejected: String) = cached.invalidate(rejected)

    private fun enc(s: String) = URLEncoder.encode(s, StandardCharsets.UTF_8)

    private companion object {
        val log = LoggerFactory.getLogger(ClientCredentialsTokenSource::class.java)
        val JSON = Moshi.Builder().build().adapter(Map::class.java)
    }
}

/**
 * Runs a command that prints an access token (e.g. a CLI that logs you in). The token's lifetime
 * isn't known, so it's reused for a conservative 5 minutes, or until the server rejects it.
 */
class CommandTokenSource(private val command: String) : TokenSource {

    private val cached = CachedToken()

    @Synchronized
    override fun token(): String {
        cached.valid()?.let { return it }
        // stdout goes to a file, so a command that never closes it can't outlast the timeout
        val out = File.createTempFile("flyte-token", ".txt")
        try {
            val proc = ProcessBuilder("bash", "-c", command)
                .redirectOutput(out)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start()
            if (!proc.waitFor(TIMEOUT.toMinutes(), TimeUnit.MINUTES)) {
                proc.destroyForcibly()
                throw FlyteException("Flyte auth command timed out after ${TIMEOUT.toMinutes()} minutes: $command")
            }
            val text = out.readText().trim()
            if (proc.exitValue() != 0 || text.isEmpty())
                throw FlyteException("Flyte auth command failed (exit ${proc.exitValue()}): $command")
            return cached.set(text.lines().last().trim(), Instant.now().plus(TTL))
        }
        finally {
            out.delete()
        }
    }

    @Synchronized
    override fun invalidate(rejected: String) = cached.invalidate(rejected)

    private companion object {
        val TTL: Duration = Duration.ofMinutes(5)
        val TIMEOUT: Duration = Duration.ofMinutes(5)
    }
}

/**
 * Adds the bearer token to every call. A call rejected with HTTP 401 gets a fresh token and is
 * sent once more, unless its body can only be sent once (a stream): its caller retries it.
 *
 * Failing to get a token is reported as an [IOException], like any other transport failure, so
 * callers retry it; OkHttp would otherwise report the call as cancelled.
 */
internal class AuthInterceptor(
    private val tokens: TokenSource,
    private val header: () -> String,
) : Interceptor {

    override fun intercept(chain: Interceptor.Chain): Response {
        val name = authenticated { header() }
        val token = authenticated { tokens.token() }
        val resp = chain.proceed(chain.request().newBuilder().header(name, "Bearer $token").build())
        if (resp.code != 401) return resp
        tokens.invalidate(token)
        if (chain.request().body?.isOneShot() == true) return resp
        resp.close()
        val fresh = authenticated { tokens.token() }
        return chain.proceed(chain.request().newBuilder().header(name, "Bearer $fresh").build())
    }

    private fun <T> authenticated(get: () -> T): T =
        try {
            get()
        }
        catch (e: IOException) {
            throw e
        }
        catch (e: Exception) {
            throw IOException("Unable to authenticate to Flyte: ${e.message}", e)
        }
}
