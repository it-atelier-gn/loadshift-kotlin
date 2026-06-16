package loadshift.core

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertEquals

@Serializable
private data class Job(var id: String, var attempts: Int = 0) : WorkItem {
    override val key get() = id
}

class WorkItemTest {

    @Test
    fun codecRoundTripPreservesValues() {
        val codec = workItemCodec<Job>()
        val job = Job("j1", 2)
        assertEquals(job, codec.decode(codec.encode(job)))
    }

    @Test
    fun codecDecodeReadsAllFields() {
        val codec = workItemCodec<Job>()
        val json = buildJsonObject {
            put("id", JsonPrimitive("j1"))
            put("attempts", JsonPrimitive(3))
        }
        val job = codec.decode(json)
        assertEquals("j1", job.id)
        assertEquals(3, job.attempts)
    }

    @Test
    fun missingFieldUsesConstructorDefault() {
        val codec = workItemCodec<Job>()
        val json = buildJsonObject { put("id", JsonPrimitive("j1")) }
        val job = codec.decode(json)
        assertEquals(0, job.attempts)
    }

    @Test
    fun keyOverrideReturnsCorrectValue() {
        assertEquals("j1", Job("j1").key)
    }
}
