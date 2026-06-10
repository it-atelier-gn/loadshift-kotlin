package loadshift.camunda7

import kotlinx.serialization.Serializable

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
data class StartInstanceResponse(
    val id: String,
    val definitionId: String,
    val businessKey: String? = null,
)

@Serializable
data class FetchTopicDto(
    val topicName: String,
    val lockDuration: Long,
    val variables: List<String>? = null,
    val deserializeValues: Boolean = false,
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
data class ExtendLockRequest(
    val workerId: String,
    val newDuration: Long,
)

@Serializable
data class CountDto(val count: Long)
