package loadshift.sqlite

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import loadshift.core.DeadLetter
import loadshift.core.DeadLetterRecord
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Instant

class SqliteDeadLetterStoreTest {

    private fun tempPath() = Files.createTempFile("dead-letters", ".db").toString()

    private fun record(
        id: String,
        millis: Long,
        workflow: String = "orders",
        key: String? = id,
        itemVariable: String? = null,
        runId: String? = null,
    ) = DeadLetterRecord(
        id = id,
        workflowKey = workflow,
        level = if (itemVariable == null) workflow else "${workflow}_f1",
        itemVariable = itemVariable,
        deadLetter = DeadLetter(key, "ship", "no carrier for $id"),
        item = JsonObject(mapOf("id" to JsonPrimitive(id), "total" to JsonPrimitive(millis))),
        recordedAt = Instant.fromEpochMilliseconds(millis),
        runId = runId,
    )

    @Test
    fun pagesTheRecordsOfOneRunAndKeepsTheRunId() = runTest {
        SqliteDeadLetterStore(tempPath()).use { store ->
            store.record(record("a", 1, runId = "r1"))
            store.record(record("b", 2, runId = "r2"))
            store.record(record("c", 3, runId = "r1"))

            val first = store.forRun("r1", limit = 1)
            assertEquals(listOf("a"), first.records.map { it.id })
            assertEquals("r1", first.records.single().runId)
            val second = store.forRun("r1", limit = 1, after = first.nextCursor)
            assertEquals(listOf("c"), second.records.map { it.id })
            assertNull(second.nextCursor)
        }
    }

    @Test
    fun storesAndReadsRecordsIncludingNullFields() = runTest {
        SqliteDeadLetterStore(tempPath()).use { store ->
            val root = record("a", 10)
            val child = record("b", 11, key = null, itemVariable = "f1_item")
            store.record(root)
            store.record(child)

            assertEquals(root, store.get("a"))
            assertEquals(child, store.get("b"))
            assertNull(store.get("missing"))
        }
    }

    @Test
    fun listsOneWorkflowAndPagesWithoutRepeatingOrSkippingWhenRecordsAreRemoved() = runTest {
        SqliteDeadLetterStore(tempPath()).use { store ->
            listOf("a" to 1L, "b" to 1L, "c" to 2L, "d" to 3L, "e" to 3L).forEach { (id, millis) -> store.record(record(id, millis)) }
            store.record(record("other", 1, workflow = "billing"))

            val first = store.list("orders", limit = 2)
            assertEquals(listOf("a", "b"), first.records.map { it.id })
            first.records.forEach { store.remove(it.id) }

            val second = store.list("orders", limit = 2, after = first.nextCursor)
            assertEquals(listOf("c", "d"), second.records.map { it.id })

            val third = store.list("orders", limit = 2, after = second.nextCursor)
            assertEquals(listOf("e"), third.records.map { it.id })
            assertNull(third.nextCursor)
            assertEquals(listOf("other"), store.list("billing").records.map { it.id })
        }
    }

    @Test
    fun findsRecordsByItemKeyInRecordingOrder() = runTest {
        SqliteDeadLetterStore(tempPath()).use { store ->
            store.record(record("second", 2, key = "a"))
            store.record(record("first", 1, key = "a"))
            store.record(record("other", 1, key = "b"))
            store.record(record("elsewhere", 1, workflow = "billing", key = "a"))

            assertEquals(listOf("first", "second"), store.forKey("orders", "a").map { it.id })
            assertEquals(emptyList(), store.forKey("orders", "missing"))
        }
    }

    @Test
    fun recordsSurviveReopen() = runTest {
        val path = tempPath()
        SqliteDeadLetterStore(path).use { it.record(record("persisted", 5)) }
        SqliteDeadLetterStore(path).use { assertEquals(record("persisted", 5), it.get("persisted")) }
    }

    @Test
    fun rejectsInvalidLimitsAndCursors() = runTest {
        SqliteDeadLetterStore(tempPath()).use { store ->
            assertFailsWith<IllegalArgumentException> { store.list("orders", limit = 0) }
            assertFailsWith<IllegalArgumentException> { store.list("orders", after = "not-a-cursor") }
        }
    }
}
