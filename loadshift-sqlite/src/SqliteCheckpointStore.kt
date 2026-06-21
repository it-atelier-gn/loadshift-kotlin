package loadshift.sqlite

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import loadshift.core.CheckpointStore
import java.sql.Connection
import java.sql.DriverManager

class SqliteCheckpointStore(path: String) : CheckpointStore, AutoCloseable {

    private val connection: Connection = DriverManager.getConnection("jdbc:sqlite:$path")
    private val mutex = Mutex()

    init {
        connection.createStatement().use { stmt ->
            stmt.execute(
                """
                CREATE TABLE IF NOT EXISTS completed (
                    run_key TEXT NOT NULL,
                    item_key TEXT NOT NULL,
                    done_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (run_key, item_key)
                )
                """.trimIndent(),
            )
        }
    }

    override suspend fun isComplete(runKey: String, itemKey: String): Boolean =
        withContext(Dispatchers.IO) {
            mutex.withLock {
                connection.prepareStatement(
                    "SELECT 1 FROM completed WHERE run_key = ? AND item_key = ? LIMIT 1",
                ).use { stmt ->
                    stmt.setString(1, runKey)
                    stmt.setString(2, itemKey)
                    stmt.executeQuery().use { it.next() }
                }
            }
        }

    override suspend fun markComplete(runKey: String, itemKey: String) {
        withContext(Dispatchers.IO) {
            mutex.withLock {
                connection.prepareStatement(
                    "INSERT OR IGNORE INTO completed (run_key, item_key) VALUES (?, ?)",
                ).use { stmt ->
                    stmt.setString(1, runKey)
                    stmt.setString(2, itemKey)
                    stmt.executeUpdate()
                }
            }
        }
    }

    override fun close() {
        connection.close()
    }
}
