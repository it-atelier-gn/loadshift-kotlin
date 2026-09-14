package loadshift.core

import kotlinx.serialization.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Instant

data class DeadLetterRecord(
    val id: String,
    val workflowKey: String,
    val level: String,
    val itemVariable: String?,
    val deadLetter: DeadLetter,
    val item: JsonObject,
    val recordedAt: Instant,
    val runId: String? = null,
) {
    val cursor: String get() = "${recordedAt.toEpochMilliseconds()}:$id"
}

data class DeadLetterPage(val records: List<DeadLetterRecord>, val nextCursor: String?)

interface DeadLetterStore {
    suspend fun record(record: DeadLetterRecord)
    suspend fun list(workflowKey: String, limit: Int = 100, after: String? = null): DeadLetterPage
    suspend fun forRun(runId: String, limit: Int = 100, after: String? = null): DeadLetterPage
    suspend fun get(id: String): DeadLetterRecord?
    suspend fun forKey(workflowKey: String, itemKey: String): List<DeadLetterRecord>
    suspend fun remove(id: String)
}

class InMemoryDeadLetterStore : DeadLetterStore {
    private val records = ConcurrentHashMap<String, DeadLetterRecord>()

    override suspend fun record(record: DeadLetterRecord) {
        records[record.id] = record
    }

    override suspend fun list(workflowKey: String, limit: Int, after: String?): DeadLetterPage =
        page(limit, after) { it.workflowKey == workflowKey }

    override suspend fun forRun(runId: String, limit: Int, after: String?): DeadLetterPage =
        page(limit, after) { it.runId == runId }

    override suspend fun get(id: String): DeadLetterRecord? = records[id]

    override suspend fun forKey(workflowKey: String, itemKey: String): List<DeadLetterRecord> =
        records.values
            .filter { it.workflowKey == workflowKey && it.deadLetter.key == itemKey }
            .sortedWith(compareBy<DeadLetterRecord> { it.recordedAt.toEpochMilliseconds() }.thenBy { it.id })

    override suspend fun remove(id: String) {
        records.remove(id)
    }

    private fun page(limit: Int, after: String?, matches: (DeadLetterRecord) -> Boolean): DeadLetterPage {
        require(limit > 0) { "limit must be positive, was $limit" }
        val position = after?.let(::parseDeadLetterCursor)
        val remaining = records.values
            .filter(matches)
            .map { it.recordedAt.toEpochMilliseconds() to it }
            .sortedWith(compareBy<Pair<Long, DeadLetterRecord>> { it.first }.thenBy { it.second.id })
            .filter { (millis, record) -> position == null || millis > position.first || (millis == position.first && record.id > position.second) }
            .map { it.second }
        val page = remaining.take(limit)
        return DeadLetterPage(page, if (remaining.size > limit) page.last().cursor else null)
    }
}

fun parseDeadLetterCursor(cursor: String): Pair<Long, String> {
    val separator = cursor.indexOf(':')
    val millis = cursor.substring(0, separator.coerceAtLeast(0)).toLongOrNull()
    require(separator > 0 && millis != null && separator < cursor.length - 1) { "invalid dead-letter cursor '$cursor'" }
    return millis to cursor.substring(separator + 1)
}
