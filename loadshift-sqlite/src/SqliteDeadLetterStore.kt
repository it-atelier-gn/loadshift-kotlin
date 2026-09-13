package loadshift.sqlite

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import loadshift.core.DeadLetter
import loadshift.core.DeadLetterPage
import loadshift.core.DeadLetterRecord
import loadshift.core.DeadLetterStore
import loadshift.core.parseDeadLetterCursor
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Types
import kotlin.time.Instant

class SqliteDeadLetterStore(path: String) : DeadLetterStore, AutoCloseable {

    private val connection: Connection = DriverManager.getConnection("jdbc:sqlite:$path")
    private val mutex = Mutex()

    init {
        connection.createStatement().use { stmt ->
            stmt.execute(
                """
                CREATE TABLE IF NOT EXISTS dead_letters (
                    id TEXT PRIMARY KEY,
                    workflow_key TEXT NOT NULL,
                    level TEXT NOT NULL,
                    item_variable TEXT,
                    item_key TEXT,
                    topic TEXT NOT NULL,
                    error TEXT NOT NULL,
                    item TEXT NOT NULL,
                    recorded_at INTEGER NOT NULL
                )
                """.trimIndent(),
            )
            stmt.execute("CREATE INDEX IF NOT EXISTS dead_letters_page ON dead_letters (workflow_key, recorded_at, id)")
        }
    }

    override suspend fun record(record: DeadLetterRecord) {
        io {
            connection.prepareStatement(
                """
                INSERT OR REPLACE INTO dead_letters
                    (id, workflow_key, level, item_variable, item_key, topic, error, item, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
            ).use { stmt ->
                stmt.setString(1, record.id)
                stmt.setString(2, record.workflowKey)
                stmt.setString(3, record.level)
                stmt.setNullableString(4, record.itemVariable)
                stmt.setNullableString(5, record.deadLetter.key)
                stmt.setString(6, record.deadLetter.topic)
                stmt.setString(7, record.deadLetter.error)
                stmt.setString(8, record.item.toString())
                stmt.setLong(9, record.recordedAt.toEpochMilliseconds())
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun list(workflowKey: String, limit: Int, after: String?): DeadLetterPage {
        require(limit > 0) { "limit must be positive, was $limit" }
        val position = after?.let(::parseDeadLetterCursor)
        return io {
            val sql = buildString {
                append("SELECT $COLUMNS FROM dead_letters WHERE workflow_key = ?")
                if (position != null) append(" AND (recorded_at > ? OR (recorded_at = ? AND id > ?))")
                append(" ORDER BY recorded_at, id LIMIT ?")
            }
            connection.prepareStatement(sql).use { stmt ->
                var index = 1
                stmt.setString(index++, workflowKey)
                if (position != null) {
                    stmt.setLong(index++, position.first)
                    stmt.setLong(index++, position.first)
                    stmt.setString(index++, position.second)
                }
                stmt.setInt(index, limit + 1)
                val rows = stmt.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.toRecord()) } }
                val page = rows.take(limit)
                DeadLetterPage(page, if (rows.size > limit) page.last().cursor else null)
            }
        }
    }

    override suspend fun get(id: String): DeadLetterRecord? = io {
        connection.prepareStatement("SELECT $COLUMNS FROM dead_letters WHERE id = ?").use { stmt ->
            stmt.setString(1, id)
            stmt.executeQuery().use { rs -> if (rs.next()) rs.toRecord() else null }
        }
    }

    override suspend fun remove(id: String) {
        io {
            connection.prepareStatement("DELETE FROM dead_letters WHERE id = ?").use { stmt ->
                stmt.setString(1, id)
                stmt.executeUpdate()
            }
        }
    }

    override fun close() {
        connection.close()
    }

    private suspend fun <T> io(block: () -> T): T = withContext(Dispatchers.IO) { mutex.withLock { block() } }

    private fun java.sql.PreparedStatement.setNullableString(index: Int, value: String?) {
        if (value == null) setNull(index, Types.VARCHAR) else setString(index, value)
    }

    private fun ResultSet.toRecord() = DeadLetterRecord(
        id = getString("id"),
        workflowKey = getString("workflow_key"),
        level = getString("level"),
        itemVariable = getString("item_variable"),
        deadLetter = DeadLetter(getString("item_key"), getString("topic"), getString("error")),
        item = Json.parseToJsonElement(getString("item")).jsonObject,
        recordedAt = Instant.fromEpochMilliseconds(getLong("recorded_at")),
    )

    private companion object {
        const val COLUMNS = "id, workflow_key, level, item_variable, item_key, topic, error, item, recorded_at"
    }
}
