package loadshift.camunda7

import loadshift.core.RetryPolicy
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class Camunda7RetryTest {

    @Test
    fun nextRetriesCountsDownToZero() {
        assertEquals(2, nextRetries(3))
        assertEquals(1, nextRetries(2))
        assertEquals(0, nextRetries(1))
        assertEquals(0, nextRetries(0))
    }

    @Test
    fun retryBackoffGrowsWithEachAttemptAndCapsAtMaxDelay() {
        val policy = RetryPolicy(maxAttempts = 5, baseDelay = 100.milliseconds, maxDelay = 1.seconds)

        assertEquals(100, retryBackoffMillis(policy, remainingBeforeFailure = 5))
        assertEquals(200, retryBackoffMillis(policy, remainingBeforeFailure = 4))
        assertEquals(400, retryBackoffMillis(policy, remainingBeforeFailure = 3))
        assertEquals(800, retryBackoffMillis(policy, remainingBeforeFailure = 2))
        assertEquals(1000, retryBackoffMillis(policy, remainingBeforeFailure = 1))
    }
}
