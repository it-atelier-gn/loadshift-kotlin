package loadshift.core

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

class ResilienceTest {

    @Test
    fun backoffDoublesPerAttemptUpToMaxDelay() {
        val policy = RetryPolicy(maxAttempts = 6, baseDelay = 100.milliseconds, maxDelay = 1.seconds, jitter = false)
        assertEquals(listOf(100, 200, 400, 800, 1000, 1000).map { it.milliseconds }, (1..6).map { policy.backoff(it) })
    }

    @Test
    fun jitterKeepsBackoffBetweenHalfAndFullDelay() {
        val policy = RetryPolicy(baseDelay = 100.milliseconds, maxDelay = 10.seconds, jitter = true)
        repeat(200) { seed ->
            val delay = policy.backoff(3, Random(seed))
            assertTrue(delay in 200.milliseconds..400.milliseconds, "attempt 3 backoff was $delay")
        }
    }

    @Test
    fun backoffStaysCappedForLargeAttemptNumbers() {
        val policy = RetryPolicy(baseDelay = 1.seconds, maxDelay = 30.seconds, jitter = false)
        assertEquals(30.seconds, policy.backoff(10_000))
    }

    @Test
    fun backoffRejectsAttemptsBelowOne() {
        assertFailsWith<IllegalArgumentException> { RetryPolicy().backoff(0) }
    }

    @Test
    fun rateLimiterSpacesPermitsEvenly() = runBlocking {
        val limiter = RateLimiter(Rate(10, 1.seconds))
        val started = TimeSource.Monotonic.markNow()
        repeat(4) { limiter.acquire() }
        assertTrue(started.elapsedNow() >= 250.milliseconds, "four permits took ${started.elapsedNow()}")
    }

    @Test
    fun taskTimeoutReturnsTheResultOfAFastBlock() = runTest {
        assertEquals(7, withTaskTimeout(1.seconds) { 7 })
        assertNull(withTaskTimeout<String?>(1.seconds) { null })
        assertEquals("direct", withTaskTimeout(null) { "direct" })
    }

    @Test
    fun taskTimeoutThrowsTaskTimeoutException() = runTest {
        val error = assertFailsWith<TaskTimeoutException> {
            withTaskTimeout(50.milliseconds) { delay(1.seconds) }
        }
        assertEquals(50.milliseconds, error.timeout)
        assertEquals("exceeded timeout 50ms", error.message)
    }

    @Test
    fun enclosingTimeoutIsNotReportedAsTaskTimeout() = runTest {
        val outcome = withTimeoutOrNull(50.milliseconds) {
            withTaskTimeout(1.seconds) { delay(10.seconds) }
            "finished"
        }
        assertNull(outcome)
    }
}
