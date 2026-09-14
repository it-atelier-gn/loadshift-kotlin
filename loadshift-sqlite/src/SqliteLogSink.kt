package loadshift.sqlite

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import loadshift.core.LogEntry
import loadshift.core.LogPage
import loadshift.core.LogReader
import loadshift.core.LogSink
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import kotlin.time.Instant

class SqliteLogSink(path: String) : LogSink, LogReader, AutoCloseable {

    private val connection: Connection = DriverManager.getConnection("jdbc:sqlite:$path")
    private val mutex = Mutex()

    init {
        connection.createStatement().use { stmt ->
            stmt.execute(
                """
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    workflow_name TEXT NOT NULL,
                    path TEXT NOT NULL,
                    item_key TEXT,
                    topic TEXT,
                    message TEXT NOT NULL,
                    data TEXT NOT NULL
                )
                """.trimIndent(),
            )
            stmt.execute("CREATE INDEX IF NOT EXISTS logs_run ON logs (run_id, id)")
        }
    }

    override suspend fun write(entry: LogEntry) {
        io {
            connection.prepareStatement(
                """
                INSERT INTO logs (timestamp, run_id, workflow_name, path, item_key, topic, message, data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
            ).use { stmt ->
                stmt.setString(1, entry.timestamp.toString())
                stmt.setString(2, entry.runId)
                stmt.setString(3, entry.workflowName)
                stmt.setString(4, JsonArray(entry.path.map { JsonPrimitive(it) }).toString())
                stmt.setString(5, entry.itemKey)
                stmt.setString(6, entry.topic)
                stmt.setString(7, entry.message)
                stmt.setString(8, JsonObject(entry.data).toString())
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun list(runId: String, limit: Int, after: String?): LogPage {
        require(limit > 0) { "limit must be positive, was $limit" }
        val position = after?.let { it.toLongOrNull() ?: throw IllegalArgumentException("invalid log cursor '$it'") }
        return io {
            connection.prepareStatement(
                "SELECT id, timestamp, run_id, workflow_name, path, item_key, topic, message, data FROM logs " +
                    "WHERE run_id = ? AND id > ? ORDER BY id LIMIT ?",
            ).use { stmt ->
                stmt.setString(1, runId)
                stmt.setLong(2, position ?: 0)
                stmt.setInt(3, limit + 1)
                val rows = stmt.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getLong("id") to rs.toEntry()) } }
                val page = rows.take(limit)
                LogPage(page.map { it.second }, if (rows.size > limit) page.last().first.toString() else null)
            }
        }
    }

    override fun close() {
        connection.close()
    }

    private suspend fun <T> io(block: () -> T): T = withContext(Dispatchers.IO) { mutex.withLock { block() } }

    private fun ResultSet.toEntry() = LogEntry(
        runId = getString("run_id"),
        workflowName = getString("workflow_name"),
        path = Json.parseToJsonElement(getString("path")).jsonArray.map { it.jsonPrimitive.content },
        itemKey = getString("item_key"),
        topic = getString("topic"),
        message = getString("message"),
        data = Json.parseToJsonElement(getString("data")).jsonObject,
        timestamp = Instant.parse(getString("timestamp")),
    )
}
