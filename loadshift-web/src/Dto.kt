package loadshift.web

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import loadshift.core.DeadLetter
import loadshift.core.DeadLetterPage
import loadshift.core.DeadLetterRecord
import loadshift.core.FlowNode
import loadshift.core.ItemStatus
import loadshift.core.LogEntry
import loadshift.core.LogPage
import loadshift.core.Progress
import loadshift.core.RunRecord
import loadshift.core.RunSnapshot
import loadshift.core.Workflow
import kotlin.time.Instant

@Serializable
data class BackendDto(val type: String, val runCount: Int)

@Serializable
data class ProgressDto(
    val seeded: Long,
    val expanded: Long,
    val done: Long,
    val failed: Long,
    val skipped: Long,
    val cancelled: Long,
)

@Serializable
data class ItemStatusDto(
    val key: String,
    val state: String,
    val topic: String?,
    val deadLetters: List<DeadLetterDto>,
)

@Serializable
data class DeadLetterDto(val key: String?, val topic: String, val error: String)

@Serializable
data class RunDto(
    val id: String,
    val workflowKey: String,
    val workflowName: String,
    val state: String,
    val startedAt: String,
    val progress: ProgressDto,
    val deadLetters: List<DeadLetterDto>,
    val engineActive: Long?,
    val deadLetterCount: Long = deadLetters.size.toLong(),
    val worker: String? = null,
    val controllable: Boolean = true,
    val updatedAt: String? = null,
    val stale: Boolean = false,
)

@Serializable
data class FlowNodeDto(
    val type: String,
    val label: String,
    val children: List<FlowNodeDto> = emptyList(),
)

@Serializable
data class RunDetailDto(val run: RunDto, val structure: FlowNodeDto?)

@Serializable
data class WorkflowDto(val key: String, val name: String)

@Serializable
data class DeadLetterRecordDto(
    val id: String,
    val workflowKey: String,
    val level: String,
    val itemVariable: String?,
    val key: String?,
    val topic: String,
    val error: String,
    val item: JsonObject,
    val recordedAt: String,
    val runId: String? = null,
)

@Serializable
data class DeadLetterPageDto(val records: List<DeadLetterRecordDto>, val nextCursor: String?)

@Serializable
data class LogEntryDto(
    val runId: String,
    val workflowName: String,
    val path: List<String>,
    val itemKey: String?,
    val topic: String?,
    val message: String,
    val data: JsonObject,
    val timestamp: String,
)

@Serializable
data class LogPageDto(val entries: List<LogEntryDto>, val nextCursor: String?)

@Serializable
data class UserTaskDto(
    val id: String,
    val workflowKey: String,
    val name: String,
    val itemKey: String?,
    val assignee: String?,
    val candidateGroups: List<String>,
)

internal fun loadshift.core.UserTask.toDto() = UserTaskDto(id, workflowKey, name, itemKey, assignee, candidateGroups)

internal fun LogEntry.toDto() = LogEntryDto(runId, workflowName, path, itemKey, topic, message, JsonObject(data), timestamp.toString())

internal fun LogPage.toDto() = LogPageDto(entries.map { it.toDto() }, nextCursor)

internal fun Progress.toDto() = ProgressDto(seeded, expanded, done, failed, skipped, cancelled)

internal fun ItemStatus.toDto() = ItemStatusDto(key, state.name, topic, deadLetters.map { it.toDto() })

internal fun DeadLetter.toDto() = DeadLetterDto(key, topic, error)

internal fun RunSnapshot.toDto(worker: String? = null) = RunDto(
    id = id,
    workflowKey = workflowKey,
    workflowName = workflowName,
    state = state.name,
    startedAt = startedAt.toString(),
    progress = progress.toDto(),
    deadLetters = deadLetters.map { it.toDto() },
    engineActive = engineActive,
    worker = worker,
)

internal fun RunRecord.toDto(now: Instant) = RunDto(
    id = id,
    workflowKey = workflowKey,
    workflowName = workflowName,
    state = state.name,
    startedAt = startedAt.toString(),
    progress = progress.toDto(),
    deadLetters = emptyList(),
    engineActive = null,
    deadLetterCount = deadLetters,
    worker = worker,
    controllable = false,
    updatedAt = updatedAt.toString(),
    stale = isStale(now),
)

internal fun FlowNode.toDto(): FlowNodeDto = FlowNodeDto(type, label, children.map { it.toDto() })

internal fun Workflow<*>.toDto() = WorkflowDto(key, name)

internal fun DeadLetterRecord.toDto() = DeadLetterRecordDto(
    id = id,
    workflowKey = workflowKey,
    level = level,
    itemVariable = itemVariable,
    key = deadLetter.key,
    topic = deadLetter.topic,
    error = deadLetter.error,
    item = item,
    recordedAt = recordedAt.toString(),
    runId = runId,
)

internal fun DeadLetterPage.toDto() = DeadLetterPageDto(records.map { it.toDto() }, nextCursor)
