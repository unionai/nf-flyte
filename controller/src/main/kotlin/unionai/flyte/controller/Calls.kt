package unionai.flyte.controller

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.ResponseMessage
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory

/** Exponential backoff: [minMillis] doubling per attempt, up to [maxMillis] */
internal fun backoffMillis(attempt: Int, minMillis: Long, maxMillis: Long): Long =
    minOf(minMillis shl minOf(attempt - 1, 20), maxMillis)

/**
 * Retry policy for unary calls: transient failures back off exponentially up to
 * [maxBackoffMillis], for up to [maxAttempts] attempts.
 *
 * @param untilAvailable keep retrying RESOURCE_EXHAUSTED past [maxAttempts]: it only means the
 *      service is shedding load
 */
internal class Retry(
    val maxAttempts: Int,
    val minBackoffMillis: Long = 500,
    val maxBackoffMillis: Long = 10_000,
    val untilAvailable: Boolean = false,
) {
    fun backoffMillis(attempt: Int): Long = backoffMillis(attempt, minBackoffMillis, maxBackoffMillis)

    companion object {
        /**
         * For enqueue and abort, as in the Flyte SDK's controller: about 20 minutes, so a
         * control plane rollout or a network blip doesn't fail a pipeline that has run for hours.
         */
        fun actions() = Retry(maxAttempts = 100, untilAvailable = true)

        /** For calls someone waits on (startup, shutdown, a task's failure details): about 15s */
        fun blocking() = Retry(maxAttempts = 5)

        /**
         * Codes worth another try. FAILED_PRECONDITION is what the Actions service returns when a
         * call reached the wrong shard, e.g. during a rebalance.
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
 * Makes unary calls with a [Retry] policy. A rejected token is replaced by the auth interceptor,
 * which sees the call's HTTP 401, so a retried call authenticates again.
 */
internal class Caller(
    private val retry: Retry,
    private val limiter: RateLimiter? = null,
) {
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
            when (val resp = call()) {
                is ResponseMessage.Success -> return Result.success(resp.message)
                is ResponseMessage.Failure -> {
                    val cause = resp.cause
                    if (cause.code in accept) return Result.failure(cause)
                    val retryable = cause.code in Retry.RETRYABLE &&
                        (attempt < retry.maxAttempts || (retry.untilAvailable && cause.code == Code.RESOURCE_EXHAUSTED))
                    if (!retryable)
                        throw FlyteException("$method failed (${cause.code.codeName}): ${cause.message ?: cause.exception?.message}", cause)
                    log.debug("[FLYTE] {} failed ({}), retrying (attempt {})", method, cause.code.codeName, attempt)
                    delay(retry.backoffMillis(attempt))
                }
            }
        }
    }

    private companion object {
        val log = LoggerFactory.getLogger(Caller::class.java)
    }
}

internal val Throwable.connectCode: Code? get() = (this as? ConnectException)?.code
