package loadshift.web

import kotlinx.serialization.Serializable
import loadshift.core.DeadLetter
import loadshift.core.FlowNode
import loadshift.core.Progress
import loadshift.core.RunSnapshot

@Serializable
data class BackendDto(val type: String, val runCount: Int)

@Serializable
data class ProgressDto(
    val seeded: Long,
    val expanded: Long,
    val done: Long,
    val failed: Long,
    val skipped: Long,
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
)

@Serializable
data class FlowNodeDto(
    val type: String,
    val label: String,
    val children: List<FlowNodeDto> = emptyList(),
)

@Serializable
data class RunDetailDto(val run: RunDto, val structure: FlowNodeDto?)

internal fun Progress.toDto() = ProgressDto(seeded, expanded, done, failed, skipped)

internal fun DeadLetter.toDto() = DeadLetterDto(key, topic, error)

internal fun RunSnapshot.toDto() = RunDto(
    id = id,
    workflowKey = workflowKey,
    workflowName = workflowName,
    state = state.name,
    startedAt = startedAt.toString(),
    progress = progress.toDto(),
    deadLetters = deadLetters.map { it.toDto() },
    engineActive = engineActive,
)

internal fun FlowNode.toDto(): FlowNodeDto = FlowNodeDto(type, label, children.map { it.toDto() })
