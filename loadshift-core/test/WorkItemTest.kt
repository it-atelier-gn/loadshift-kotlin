package loadshift.core

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private class Job : WorkItemBase() {
    var attempts: Int by required(default = 0)
    var id: String by required()
    override val key get() = id
}

class WorkItemTest {

    @Test
    fun requiredWithDefaultReturnsDefaultUntilSet() {
        val job = Job().apply { id = "j1" }
        assertEquals(0, job.attempts)

        job.attempts = job.attempts + 1
        assertEquals(1, job.attempts)
    }

    @Test
    fun requiredWithoutDefaultErrorsWhenUnset() {
        val job = Job()
        assertFailsWith<IllegalStateException> { job.id }
    }

    @Test
    fun defaultValuesSurviveToMapAndHydrateRoundTrip() {
        val job = Job().apply { id = "j1" }
        job.attempts = job.attempts + 1

        val restored = Job()
        restored.hydrate(job.toMap())

        assertEquals("j1", restored.id)
        assertEquals(1, restored.attempts)
    }

    @Test
    fun unsetDefaultIsOmittedFromMapButStillReadsAsDefault() {
        val job = Job().apply { id = "j1" }

        assertEquals(setOf("id"), job.toMap().keys)
        assertEquals(0, job.attempts)
    }
}
