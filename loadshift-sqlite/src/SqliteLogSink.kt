package loadshift.sqlite

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import loadshift.core.LogEntry
import loadshift.core.LogSink
import java.sql.Connection
import java.sql.DriverManager

class SqliteLogSink(path: String) : LogSink, AutoCloseable {

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
        }
    }

    override suspend fun write(entry: LogEntry) {
        withContext(Dispatchers.IO) {
            mutex.withLock {
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
    }

    override fun close() {
        connection.close()
    }
}
