package unionai.flyte.controller

/**
 * Supplies the bearer token sent with every Flyte call, for a [FlyteConnection] built with
 * [FlyteConnection.Builder.tokenSource].
 *
 * Called from many threads at once, and from OkHttp's interceptor chain: implementations must be
 * thread-safe, and should cache the token rather than fetch one per call. A failure to get a token
 * (any exception) fails the call like a network error, so the call is retried.
 *
 * A Java lambda works when the token never needs replacing early:
 * ```java
 * TokenSource tokens = () -> System.getenv("FLYTE_TOKEN");
 * ```
 */
public fun interface TokenSource {

    /** A valid token, fetching a new one when there is none or it expired */
    public fun token(): String

    /**
     * The server rejected [rejected] (HTTP 401): drop it, unless another caller already replaced
     * it, so that concurrent calls rejected with the same token fetch a new one only once. The
     * default keeps the token.
     */
    public fun invalidate(rejected: String) {}
}
