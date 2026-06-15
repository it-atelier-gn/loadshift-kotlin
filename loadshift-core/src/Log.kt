package loadshift.core

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
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
