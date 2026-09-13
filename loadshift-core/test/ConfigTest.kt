package loadshift.core

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ConfigTest {

    @Test
    fun rateRequiresPositivePermitsAndWindow() {
        assertFailsWith<IllegalArgumentException> { Rate(0, 1.seconds) }
        assertFailsWith<IllegalArgumentException> { Rate(1, Duration.ZERO) }
        assertEquals(Rate(5, 1.seconds), perSecond(5))
    }

    @Test
    fun retryPolicyValidatesItsBounds() {
        assertFailsWith<IllegalArgumentException> { RetryPolicy(maxAttempts = 0) }
        assertFailsWith<IllegalArgumentException> { RetryPolicy(baseDelay = (-1).milliseconds) }
        assertFailsWith<IllegalArgumentException> { RetryPolicy(baseDelay = 2.seconds, maxDelay = 1.seconds) }
        assertFailsWith<IllegalArgumentException> { RetryPolicy(timeout = Duration.ZERO) }
        assertEquals(1, RetryPolicy.None.maxAttempts)
    }

    @Test
    fun taskOptionsRejectNonPositiveTimeouts() {
        assertFailsWith<IllegalArgumentException> { TaskOptions(timeout = Duration.ZERO) }
    }

    @Test
    fun runConfigValidatesLimits() {
        assertFailsWith<IllegalArgumentException> { RunConfig(maxConcurrency = 0) }
        assertFailsWith<IllegalArgumentException> { RunConfig(lockDuration = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { RunConfig(maxLoopIterations = 0) }
        RunConfig()
    }
}
