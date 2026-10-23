package unionai.flyte.controller

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.ResponseMessage
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory

/**
 * Retry policy for unary calls, as in the Flyte SDK's controller: transient failures back off
 * exponentially up to [maxBackoffMillis], for up to [maxAttempts] attempts (about 20 minutes),
 * so a control plane rollout or a network blip doesn't fail a pipeline that has run for hours.
 */
internal class Retry(
    val maxAttempts: Int = 100,
    val minBackoffMillis: Long = 500,
    val maxBackoffMillis: Long = 10_000,
) {
    fun backoffMillis(attempt: Int): Long =
        minOf(minBackoffMillis shl minOf(attempt - 1, 20), maxBackoffMillis)

    companion object {
        /**
         * Codes worth another try. FAILED_PRECONDITION is what the Actions service returns when a
         * call reached the wrong shard, e.g. during a rebalance. RESOURCE_EXHAUSTED is retried
         * without limit: it only means the service is shedding load.
         */
        val RETRYABLE = setOf(
            Code.UNAVAILABLE,
            Code.DEADLINE_EXCEEDED,
            Code.RESOURCE_EXHAUSTED,
            Code.FAILED_PRECONDITION,
            Code.INTERNAL_ERROR,
            Code.UNKNOWN,
            Code.UNAUTHENTICATED,
        )
    }
}

/**
 * Spaces calls to at most [perSecond] a second.
 */
internal class RateLimiter(perSecond: Int) {
    private val intervalNanos = 1_000_000_000L / perSecond
    private val mutex = Mutex()
    private var next = 0L

    suspend fun acquire() {
        val wait = mutex.withLock {
            val now = System.nanoTime()
            val at = maxOf(next, now)
            next = at + intervalNanos
            at - now
        }
        if (wait > 0) delay(wait / 1_000_000)
    }
}

/**
 * Makes unary calls with the [Retry] policy.
 *
 * @param tokens dropped when the server rejects one, so the retry authenticates again
 * @param onStaleConnection called after [STALE_AFTER] deadlines in a row: the calls may be riding
 *      a dead pooled connection (a severed NAT flow drops packets without a reset), which only
 *      a new connection gets around
 */
internal class Caller(
    private val retry: Retry = Retry(),
    private val tokens: TokenSource? = null,
    private val limiter: RateLimiter? = null,
    private val onStaleConnection: () -> Unit = {},
) {
    private val deadlines = AtomicInteger()

    /**
     * @return the response, or the failure when the call failed with one of [accept]
     */
    suspend fun <T> unary(
        method: String,
        accept: Set<Code> = emptySet(),
        call: suspend () -> ResponseMessage<T>,
    ): Result<T> {
        var attempt = 0
        while (true) {
            attempt++
            limiter?.acquire()
            val resp = call()
            val code = (resp as? ResponseMessage.Failure)?.cause?.code
            if (code == Code.DEADLINE_EXCEEDED) {
                if (deadlines.incrementAndGet() >= STALE_AFTER) {
                    deadlines.set(0)
                    log.warn("[FLYTE] {} calls in a row timed out, dropping pooled connections", STALE_AFTER)
                    onStaleConnection()
                }
            }
            else {
                deadlines.set(0)
            }
            when (resp) {
                is ResponseMessage.Success -> return Result.success(resp.message)
                is ResponseMessage.Failure -> {
                    val cause = resp.cause
                    if (cause.code in accept) return Result.failure(cause)
                    if (cause.code == Code.UNAUTHENTICATED) tokens?.invalidate()
                    val retryable = cause.code in Retry.RETRYABLE &&
                        (attempt < retry.maxAttempts || cause.code == Code.RESOURCE_EXHAUSTED)
                    if (!retryable)
                        throw FlyteException("$method failed (${cause.code.codeName}): ${cause.message ?: cause.exception?.message}", cause)
                    log.debug("[FLYTE] {} failed ({}), retrying (attempt {})", method, cause.code.codeName, attempt)
                    delay(retry.backoffMillis(attempt))
                }
            }
        }
    }

    private companion object {
        const val STALE_AFTER = 3
        val log = LoggerFactory.getLogger(Caller::class.java)
    }
}

internal val Throwable.connectCode: Code? get() = (this as? ConnectException)?.code
