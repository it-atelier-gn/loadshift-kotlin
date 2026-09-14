package loadshift.sqlite

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import loadshift.core.Progress
import loadshift.core.RunRecord
import loadshift.core.RunRegistry
import loadshift.core.RunState
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import kotlin.time.Instant

class SqliteRunRegistry(path: String) : RunRegistry, AutoCloseable {

    private val connection: Connection = DriverManager.getConnection("jdbc:sqlite:$path")
    private val mutex = Mutex()

    init {
        connection.createStatement().use { stmt ->
            stmt.execute("PRAGMA busy_timeout = $BUSY_TIMEOUT_MILLIS")
            stmt.execute("PRAGMA journal_mode = WAL")
            stmt.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    worker TEXT NOT NULL,
                    backend_type TEXT NOT NULL,
                    workflow_key TEXT NOT NULL,
                    workflow_name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    started_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    stale_at INTEGER NOT NULL,
                    seeded INTEGER NOT NULL,
                    expanded INTEGER NOT NULL,
                    done INTEGER NOT NULL,
                    failed INTEGER NOT NULL,
                    skipped INTEGER NOT NULL,
                    cancelled INTEGER NOT NULL,
                    dead_letters INTEGER NOT NULL
                )
                """.trimIndent(),
            )
            stmt.execute("CREATE INDEX IF NOT EXISTS runs_started ON runs (started_at, id)")
        }
    }

    override suspend fun save(record: RunRecord) {
        io {
            connection.prepareStatement("INSERT OR REPLACE INTO runs ($COLUMNS) VALUES (${COLUMNS.split(",").joinToString { "?" }})")
                .use { stmt ->
                    stmt.setString(1, record.id)
                    stmt.setString(2, record.worker)
                    stmt.setString(3, record.backendType)
                    stmt.setString(4, record.workflowKey)
                    stmt.setString(5, record.workflowName)
                    stmt.setString(6, record.state.name)
                    stmt.setLong(7, record.startedAt.toEpochMilliseconds())
                    stmt.setLong(8, record.updatedAt.toEpochMilliseconds())
                    stmt.setLong(9, record.staleAt.toEpochMilliseconds())
                    stmt.setLong(10, record.progress.seeded)
                    stmt.setLong(11, record.progress.expanded)
                    stmt.setLong(12, record.progress.done)
                    stmt.setLong(13, record.progress.failed)
                    stmt.setLong(14, record.progress.skipped)
                    stmt.setLong(15, record.progress.cancelled)
                    stmt.setLong(16, record.deadLetters)
                    stmt.executeUpdate()
                }
        }
    }

    override suspend fun list(limit: Int): List<RunRecord> {
        require(limit > 0) { "limit must be positive, was $limit" }
        return io {
            connection.prepareStatement("SELECT $COLUMNS FROM runs ORDER BY started_at DESC, id LIMIT ?").use { stmt ->
                stmt.setInt(1, limit)
                stmt.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.toRecord()) } }
            }
        }
    }

    override suspend fun get(id: String): RunRecord? = io {
        connection.prepareStatement("SELECT $COLUMNS FROM runs WHERE id = ?").use { stmt ->
            stmt.setString(1, id)
            stmt.executeQuery().use { rs -> if (rs.next()) rs.toRecord() else null }
        }
    }

    override suspend fun remove(id: String) {
        io {
            connection.prepareStatement("DELETE FROM runs WHERE id = ?").use { stmt ->
                stmt.setString(1, id)
                stmt.executeUpdate()
            }
        }
    }

    override fun close() {
        connection.close()
    }

    private suspend fun <T> io(block: () -> T): T = withContext(Dispatchers.IO) { mutex.withLock { block() } }

    private fun ResultSet.toRecord() = RunRecord(
        id = getString("id"),
        worker = getString("worker"),
        backendType = getString("backend_type"),
        workflowKey = getString("workflow_key"),
        workflowName = getString("workflow_name"),
        state = RunState.valueOf(getString("state")),
        startedAt = Instant.fromEpochMilliseconds(getLong("started_at")),
        updatedAt = Instant.fromEpochMilliseconds(getLong("updated_at")),
        staleAt = Instant.fromEpochMilliseconds(getLong("stale_at")),
        progress = Progress(
            seeded = getLong("seeded"),
            expanded = getLong("expanded"),
            done = getLong("done"),
            failed = getLong("failed"),
            skipped = getLong("skipped"),
            cancelled = getLong("cancelled"),
        ),
        deadLetters = getLong("dead_letters"),
    )

    private companion object {
        const val BUSY_TIMEOUT_MILLIS = 5000
        const val COLUMNS = "id, worker, backend_type, workflow_key, workflow_name, state, started_at, updated_at, stale_at, " +
            "seeded, expanded, done, failed, skipped, cancelled, dead_letters"
    }
}
