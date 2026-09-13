package loadshift.core

import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds

class RateLimiter(rate: Rate) {
    private val intervalNanos = rate.per.inWholeNanoseconds / rate.permits
    private val lock = Any()
    private var next: Long? = null

    suspend fun acquire() {
        val waitNanos = synchronized(lock) {
            val now = System.nanoTime()
            val at = next?.let { maxOf(now, it) } ?: now
            next = at + intervalNanos
            at - now
        }
        if (waitNanos > 0) delay(waitNanos.nanoseconds)
    }
}

class TaskTimeoutException(val timeout: Duration) : RuntimeException("exceeded timeout $timeout")

suspend fun <T> withTaskTimeout(timeout: Duration?, block: suspend () -> T): T {
    if (timeout == null) return block()
    val outcome = withTimeoutOrNull(timeout) { Outcome(block()) } ?: throw TaskTimeoutException(timeout)
    return outcome.value
}

private class Outcome<T>(val value: T)

fun RetryPolicy.backoff(attempt: Int, random: Random = Random.Default): Duration {
    require(attempt >= 1) { "attempt must be at least 1, was $attempt" }
    val exponential = baseDelay.inWholeMilliseconds * 2.0.pow((attempt - 1).coerceAtMost(62))
    val capped = minOf(exponential, maxDelay.inWholeMilliseconds.toDouble())
    val millis = if (jitter) capped * random.nextDouble(0.5, 1.0) else capped
    return millis.toLong().milliseconds
}
