package loadshift.sqlite

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonPrimitive
import loadshift.core.LogEntry
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Instant

class SqliteLogReaderTest {

    @Test
    fun readsTheEntriesOfOneRunInWritingOrderPageByPage() = runTest {
        SqliteLogSink(Files.createTempFile("logs", ".db").toString()).use { sink ->
            val first = LogEntry(
                runId = "r1",
                workflowName = "orders",
                path = listOf("parent"),
                itemKey = "a",
                topic = "ship",
                message = "one",
                data = mapOf("attempt" to JsonPrimitive(1), "carrier" to JsonPrimitive("dhl")),
                timestamp = Instant.parse("2026-09-14T10:00:00.123456Z"),
            )
            sink.write(first)
            sink.write(first.copy(runId = "r2", message = "other run"))
            sink.write(first.copy(message = "two", path = emptyList(), itemKey = null, topic = null, data = emptyMap()))

            val page = sink.list("r1", limit = 1)
            assertEquals(listOf(first), page.entries)
            val rest = sink.list("r1", limit = 5, after = page.nextCursor)
            assertEquals(listOf("two"), rest.entries.map { it.message })
            assertNull(rest.entries.single().itemKey)
            assertNull(rest.nextCursor)
            assertFailsWith<IllegalArgumentException> { sink.list("r1", after = "not-a-cursor") }
        }
    }
}
