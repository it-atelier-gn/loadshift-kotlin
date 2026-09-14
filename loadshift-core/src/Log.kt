package loadshift.core

import kotlinx.coroutines.currentCoroutineContext
import kotlin.time.Clock
import kotlin.time.Instant
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

data class LogEntry(
    val runId: String,
    val workflowName: String,
    val path: List<String>,
    val itemKey: String?,
    val topic: String?,
    val message: String,
    val data: Map<String, JsonElement>,
    val timestamp: Instant,
)

interface LogSink {
    suspend fun write(entry: LogEntry)
}

object NoopLogSink : LogSink {
    override suspend fun write(entry: LogEntry) {}
}

data class LogPage(val entries: List<LogEntry>, val nextCursor: String?)

interface LogReader {
    suspend fun list(runId: String, limit: Int = 100, after: String? = null): LogPage
}

class InMemoryLogSink(private val capacity: Int = DEFAULT_CAPACITY) : LogSink, LogReader {
    private val entries = ArrayDeque<Pair<Long, LogEntry>>()
    private var sequence = 0L

    init {
        require(capacity > 0) { "capacity must be positive, was $capacity" }
    }

    override suspend fun write(entry: LogEntry) {
        synchronized(entries) {
            entries.addLast(++sequence to entry)
            while (entries.size > capacity) entries.removeFirst()
        }
    }

    override suspend fun list(runId: String, limit: Int, after: String?): LogPage {
        require(limit > 0) { "limit must be positive, was $limit" }
        val position = after?.let { it.toLongOrNull() ?: throw IllegalArgumentException("invalid log cursor '$it'") }
        val matching = synchronized(entries) {
            entries.filter { (number, entry) -> entry.runId == runId && (position == null || number > position) }
        }
        val page = matching.take(limit)
        return LogPage(page.map { it.second }, if (matching.size > limit) page.last().first.toString() else null)
    }

    companion object {
        const val DEFAULT_CAPACITY = 10_000
    }
}

class ExecutionContext(
    val runId: String,
    val workflowName: String,
    val sink: LogSink,
    val path: List<String> = emptyList(),
    val itemKey: String? = null,
    val topic: String? = null,
) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<ExecutionContext>

    fun child(childItemKey: String): ExecutionContext =
        ExecutionContext(runId, workflowName, sink, path + (itemKey ?: "?"), childItemKey, null)

    fun withTopic(topic: String): ExecutionContext =
        ExecutionContext(runId, workflowName, sink, path, itemKey, topic)
}

suspend fun log(message: String, vararg data: Pair<String, Any?>) {
    val ctx = currentCoroutineContext()[ExecutionContext.Key] ?: return
    ctx.sink.write(
        LogEntry(
            runId = ctx.runId,
            workflowName = ctx.workflowName,
            path = ctx.path,
            itemKey = ctx.itemKey,
            topic = ctx.topic,
            message = message,
            data = data.toMap().mapValues { it.value.toJson() },
            timestamp = Clock.System.now(),
        ),
    )
}

private fun Any?.toJson(): JsonElement = when (this) {
    null -> JsonNull
    is JsonElement -> this
    is String -> JsonPrimitive(this)
    is Boolean -> JsonPrimitive(this)
    is Number -> JsonPrimitive(this)
    is Map<*, *> -> JsonObject(entries.associate { (k, v) -> k.toString() to v.toJson() })
    is Iterable<*> -> JsonArray(map { it.toJson() })
    else -> JsonPrimitive(toString())
}
