package loadshift.core

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Instant

class ItemsTest {

    @Test
    fun finishedItemsKeepTheMostRecentEntriesUpToCapacity() {
        val finished = FinishedItems(capacity = 2)
        finished.record("a", ItemState.Done)
        finished.record("b", ItemState.Skipped)
        finished.record("a", ItemState.DeadLettered)
        finished.record("c", ItemState.Cancelled)

        assertNull(finished["b"])
        assertEquals(ItemState.DeadLettered, finished["a"])
        assertEquals(ItemState.Cancelled, finished["c"])
        assertFailsWith<IllegalArgumentException> { FinishedItems(capacity = 0) }
    }

    @Test
    fun deadLetterStoreFindsRecordsByItemKey() = runTest {
        val store = InMemoryDeadLetterStore()
        fun record(id: String, key: String?, workflow: String = "orders", millis: Long = 0) = DeadLetterRecord(
            id, workflow, workflow, null, DeadLetter(key, "t", "e"), JsonObject(emptyMap()), Instant.fromEpochMilliseconds(millis),
        )
        store.record(record("2", "a", millis = 2))
        store.record(record("1", "a", millis = 1))
        store.record(record("3", "b"))
        store.record(record("4", "a", workflow = "billing"))
        store.record(record("5", null))

        assertEquals(listOf("1", "2"), store.forKey("orders", "a").map { it.id })
        assertEquals(emptyList(), store.forKey("orders", "missing"))
    }
}
