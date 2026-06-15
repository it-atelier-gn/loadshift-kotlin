package loadshift.sqlite

import kotlinx.coroutines.test.runTest
import kotlinx.datetime.Clock
import kotlinx.serialization.json.JsonPrimitive
import loadshift.core.LogEntry
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SqliteLogSinkTest {

    @Test
    fun writesAndPersistsLogEntries() = runTest {
        val file = Files.createTempFile("loadshift-log", ".db")
        file.toFile().deleteOnExit()
        val sink = SqliteLogSink(file.toString())

        sink.write(
            LogEntry(
                runId = "run-1",
                workflowName = "demo",
                path = listOf("a", "b"),
                itemKey = "item-1",
                topic = "do_thing",
                message = "hello",
                data = mapOf("count" to JsonPrimitive(42)),
                timestamp = Clock.System.now(),
            ),
        )
        sink.close()

        DriverManager.getConnection("jdbc:sqlite:$file").use { conn ->
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery(
                    "SELECT run_id, workflow_name, path, item_key, topic, message, data FROM logs",
                )
                assertTrue(rs.next())
                assertEquals("run-1", rs.getString("run_id"))
                assertEquals("demo", rs.getString("workflow_name"))
                assertEquals("[\"a\",\"b\"]", rs.getString("path"))
                assertEquals("item-1", rs.getString("item_key"))
                assertEquals("do_thing", rs.getString("topic"))
                assertEquals("hello", rs.getString("message"))
                assertEquals("{\"count\":42}", rs.getString("data"))
                assertFalse(rs.next())
            }
        }
    }
}
