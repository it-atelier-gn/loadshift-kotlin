package loadshift.core

import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Clock
import kotlin.time.Instant

data class RunRecord(
    val id: String,
    val worker: String,
    val backendType: String,
    val workflowKey: String,
    val workflowName: String,
    val state: RunState,
    val startedAt: Instant,
    val updatedAt: Instant,
    val staleAt: Instant,
    val progress: Progress,
    val deadLetters: Long,
) {
    fun isStale(now: Instant = Clock.System.now()): Boolean = !state.terminal && now > staleAt
}

interface RunRegistry {
    suspend fun save(record: RunRecord)
    suspend fun list(limit: Int = 1000): List<RunRecord>
    suspend fun get(id: String): RunRecord?
    suspend fun remove(id: String)
}

class InMemoryRunRegistry : RunRegistry {
    private val records = ConcurrentHashMap<String, RunRecord>()

    override suspend fun save(record: RunRecord) {
        records[record.id] = record
    }

    override suspend fun list(limit: Int): List<RunRecord> {
        require(limit > 0) { "limit must be positive, was $limit" }
        return records.values.sortedWith(compareByDescending<RunRecord> { it.startedAt }.thenBy { it.id }).take(limit)
    }

    override suspend fun get(id: String): RunRecord? = records[id]

    override suspend fun remove(id: String) {
        records.remove(id)
    }
}

fun defaultWorkerName(): String {
    val host = runCatching { InetAddress.getLocalHost().hostName }.getOrNull()?.takeIf { it.isNotBlank() } ?: "worker"
    return "$host-${ProcessHandle.current().pid()}-${Clock.System.now().toEpochMilliseconds().toString(36)}"
}
