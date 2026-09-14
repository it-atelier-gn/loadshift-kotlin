package loadshift.camunda7

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject

@Serializable
data class DeploymentDto(
    val id: String,
    val name: String? = null,
    val deploymentTime: String? = null,
)

@Serializable
data class StartInstanceRequest(
    val variables: Map<String, CamundaValue>,
    val businessKey: String? = null,
)

@Serializable
data class MessageRequest(
    val messageName: String,
    val correlationKeys: Map<String, CamundaValue>? = null,
    val all: Boolean = true,
    val resultEnabled: Boolean = false,
    val tenantId: String? = null,
)

@Serializable
data class SignalRequest(
    val name: String,
    val tenantId: String? = null,
)

@Serializable
data class StartInstanceResponse(
    val id: String,
    val definitionId: String,
    val businessKey: String? = null,
)

@Serializable
data class ProcessInstanceDto(
    val id: String,
    val businessKey: String? = null,
    val definitionId: String? = null,
)

@Serializable
data class TaskDto(
    val id: String,
    val name: String? = null,
    val assignee: String? = null,
    val taskDefinitionKey: String? = null,
    val processInstanceId: String? = null,
    val processDefinitionId: String? = null,
)

@Serializable
data class CompleteTaskRequest(val variables: Map<String, CamundaValue>)

@Serializable
data class ProcessDefinitionDto(
    val id: String,
    val key: String,
    val version: Int,
    val versionTag: String? = null,
)

@Serializable
data class MigrationGenerateRequest(
    val sourceProcessDefinitionId: String,
    val targetProcessDefinitionId: String,
    val updateEventTriggers: Boolean = false,
)

@Serializable
data class MigrationExecuteRequest(
    val migrationPlan: JsonObject,
    val processInstanceIds: List<String>,
    val skipCustomListeners: Boolean = true,
)

@Serializable
data class VariableInstanceQuery(
    val variableName: String,
    val processInstanceIdIn: List<String>,
)

@Serializable
data class VariableInstanceDto(
    val processInstanceId: String? = null,
    val value: JsonElement = JsonNull,
    val type: String? = null,
)

@Serializable
data class FetchTopicDto(
    val topicName: String,
    val lockDuration: Long,
    val variables: List<String>? = null,
    val deserializeValues: Boolean = false,
    val tenantIdIn: List<String>? = null,
)

@Serializable
data class FetchAndLockRequest(
    val workerId: String,
    val maxTasks: Int,
    val usePriority: Boolean = false,
    val asyncResponseTimeout: Long? = null,
    val topics: List<FetchTopicDto>,
)

@Serializable
data class ExternalTaskDto(
    val id: String,
    val topicName: String,
    val processInstanceId: String,
    val processDefinitionKey: String? = null,
    val retries: Int? = null,
    val variables: Map<String, CamundaValue> = emptyMap(),
)

@Serializable
data class CompleteRequest(
    val workerId: String,
    val variables: Map<String, CamundaValue> = emptyMap(),
)

@Serializable
data class FailureRequest(
    val workerId: String,
    val errorMessage: String,
    val errorDetails: String? = null,
    val retries: Int,
    val retryTimeout: Long,
)

@Serializable
data class BpmnErrorRequest(
    val workerId: String,
    val errorCode: String,
    val errorMessage: String? = null,
    val variables: Map<String, CamundaValue>? = null,
)

@Serializable
data class ExtendLockRequest(
    val workerId: String,
    val newDuration: Long,
)

@Serializable
data class HistoricProcessInstanceQuery(
    val processInstanceIds: List<String>,
    val finished: Boolean = true,
)

@Serializable
data class HistoricProcessInstanceDto(
    val id: String,
    val state: String? = null,
)

@Serializable
data class CountDto(val count: Long)
