package loadshift.core

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Instant

@Serializable
private data class Note(val id: String) : WorkItem {
    override val key get() = id
}

class RunDrillDownTest {

    private fun entry(runId: String, message: String) =
        LogEntry(runId, "wf", emptyList(), null, null, message, emptyMap(), Instant.fromEpochSeconds(0))

    private fun record(id: String, runId: String?, millis: Long) =
        DeadLetterRecord(id, "wf", "wf", null, DeadLetter(id, "t", "e"), JsonObject(emptyMap()), Instant.fromEpochMilliseconds(millis), runId)

    @Test
    fun inMemoryLogSinkPagesTheEntriesOfOneRunInWritingOrder() = runTest {
        val sink = InMemoryLogSink()
        sink.write(entry("r1", "a"))
        sink.write(entry("r2", "x"))
        sink.write(entry("r1", "b"))
        sink.write(entry("r1", "c"))

        val first = sink.list("r1", limit = 2)
        assertEquals(listOf("a", "b"), first.entries.map { it.message })
        val second = sink.list("r1", limit = 2, after = first.nextCursor)
        assertEquals(listOf("c"), second.entries.map { it.message })
        assertNull(second.nextCursor)
        assertFailsWith<IllegalArgumentException> { sink.list("r1", after = "not-a-cursor") }
        assertFailsWith<IllegalArgumentException> { sink.list("r1", limit = 0) }
        assertFailsWith<IllegalArgumentException> { InMemoryLogSink(capacity = 0) }
    }

    @Test
    fun inMemoryLogSinkDropsTheOldestEntriesBeyondItsCapacity() = runTest {
        val sink = InMemoryLogSink(capacity = 2)
        listOf("a", "b", "c").forEach { sink.write(entry("r1", it)) }

        assertEquals(listOf("b", "c"), sink.list("r1").entries.map { it.message })
    }

    @Test
    fun deadLetterStorePagesTheRecordsOfOneRun() = runTest {
        val store = InMemoryDeadLetterStore()
        store.record(record("a", "r1", 1))
        store.record(record("b", "r2", 2))
        store.record(record("c", "r1", 3))
        store.record(record("d", null, 4))

        val first = store.forRun("r1", limit = 1)
        assertEquals(listOf("a"), first.records.map { it.id })
        val second = store.forRun("r1", limit = 1, after = first.nextCursor)
        assertEquals(listOf("c"), second.records.map { it.id })
        assertNull(second.nextCursor)
    }

    @Test
    fun trackerSnapshotsCarryTheRunIdOfTheInspector() = runTest {
        val tracker = RunTracker("test")
        val id = tracker.track(
            workflow<Note>("notes") {
                input(emptyList())
                task("write") { }
            },
            object : RunInspector {
                override fun state() = RunState.Running
                override fun progress() = Progress()
                override fun runId() = "internal-1"
            },
        )

        assertEquals("internal-1", tracker.run(id)?.runId)
    }
}
