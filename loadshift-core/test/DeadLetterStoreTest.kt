package loadshift.core

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Instant

class DeadLetterStoreTest {

    private fun record(id: String, millis: Long, workflow: String = "orders") = DeadLetterRecord(
        id = id,
        workflowKey = workflow,
        level = workflow,
        itemVariable = null,
        deadLetter = DeadLetter(id, "ship", "no carrier"),
        item = JsonObject(mapOf("id" to JsonPrimitive(id))),
        recordedAt = Instant.fromEpochMilliseconds(millis),
    )

    @Test
    fun listsRecordsOfOneWorkflowInRecordingOrder() = runTest {
        val store = InMemoryDeadLetterStore()
        store.record(record("b", 2))
        store.record(record("a", 1))
        store.record(record("other", 1, workflow = "billing"))

        val page = store.list("orders")

        assertEquals(listOf("a", "b"), page.records.map { it.id })
        assertNull(page.nextCursor)
    }

    @Test
    fun pagesWithCursorsWithoutRepeatingOrSkippingWhenRecordsAreRemoved() = runTest {
        val store = InMemoryDeadLetterStore()
        listOf("a" to 1L, "b" to 1L, "c" to 2L, "d" to 3L, "e" to 3L).forEach { (id, millis) -> store.record(record(id, millis)) }

        val first = store.list("orders", limit = 2)
        assertEquals(listOf("a", "b"), first.records.map { it.id })
        first.records.forEach { store.remove(it.id) }

        val second = store.list("orders", limit = 2, after = first.nextCursor)
        assertEquals(listOf("c", "d"), second.records.map { it.id })

        val third = store.list("orders", limit = 2, after = second.nextCursor)
        assertEquals(listOf("e"), third.records.map { it.id })
        assertNull(third.nextCursor)
    }

    @Test
    fun getsAndRemovesRecordsById() = runTest {
        val store = InMemoryDeadLetterStore()
        val stored = record("a", 1)
        store.record(stored)

        assertEquals(stored, store.get("a"))
        store.remove("a")
        store.remove("missing")
        assertNull(store.get("a"))
    }

    @Test
    fun rejectsInvalidLimitsAndCursors() = runTest {
        val store = InMemoryDeadLetterStore()
        assertFailsWith<IllegalArgumentException> { store.list("orders", limit = 0) }
        for (cursor in listOf("", "abc", "12", "12:", ":a")) {
            assertFailsWith<IllegalArgumentException>(cursor) { store.list("orders", after = cursor) }
        }
    }
}
