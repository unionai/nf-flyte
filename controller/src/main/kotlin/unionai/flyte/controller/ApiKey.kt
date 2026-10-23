package unionai.flyte.controller

import java.nio.charset.StandardCharsets
import java.util.Base64

/**
 * A Flyte API key, as issued by the Flyte CLI: `base64("endpoint:client_id:client_secret:org")`.
 *
 * [FlyteConnection.Builder.apiKey] takes the encoded key; decode it to read the endpoint or org
 * it was issued for.
 */
public class ApiKey(
    public val clientId: String,
    public val clientSecret: String,
    /** Endpoint the key was issued for, e.g. `dns:///tenant.hosted.unionai.cloud` */
    public val endpoint: String?,
    /** Org the key was issued for, or null when it has none */
    public val org: String?,
) {
    override fun toString(): String = "ApiKey(clientId=$clientId, endpoint=$endpoint, org=$org)"

    public companion object {
        /**
         * @throws FlyteException when the key is missing or malformed
         */
        @JvmStatic
        public fun decode(key: String?): ApiKey {
            if (key.isNullOrBlank()) throw FlyteException("Missing Flyte API key")
            val decoded = try {
                String(Base64.getDecoder().decode(key.trim()), StandardCharsets.UTF_8)
            }
            catch (e: IllegalArgumentException) {
                throw FlyteException("Invalid Flyte API key: not base64", e)
            }
            // the endpoint may itself contain colons (e.g. dns:///host), so split from the right
            val parts = decoded.split(':')
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
