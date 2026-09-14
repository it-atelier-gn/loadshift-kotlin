package loadshift.camunda8

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

@Serializable
data class CreateInstanceRequest(
    val processDefinitionId: String,
    val variables: JsonObject,
    val tenantId: String? = null,
)

@Serializable
data class CreateInstanceResponse(val processInstanceKey: String)

@Serializable
data class ActivateJobsRequest(
    val type: String,
    val worker: String,
    val timeout: Long,
    val maxJobsToActivate: Int,
    val requestTimeout: Long = 0,
    val tenantIds: List<String>? = null,
)

@Serializable
data class ActivatedJob(
    val jobKey: String,
    val processInstanceKey: String,
    val retries: Int? = null,
    val variables: JsonObject = JsonObject(emptyMap()),
)

@Serializable
data class ActivateJobsResponse(val jobs: List<ActivatedJob> = emptyList())

@Serializable
data class CompleteJobRequest(val variables: JsonObject)

@Serializable
data class FailJobRequest(
    val retries: Int,
    val errorMessage: String? = null,
    val retryBackOff: Long = 0,
)

@Serializable
data class JobErrorRequest(
    val errorCode: String,
    val errorMessage: String? = null,
    val variables: JsonObject? = null,
)

@Serializable
data class JobChangeset(
    val retries: Int? = null,
    val timeout: Long? = null,
)

@Serializable
data class JobUpdateRequest(val changeset: JobChangeset)

@Serializable
data class MessageCorrelationRequest(
    val name: String,
    val correlationKey: String,
    val tenantId: String? = null,
)

@Serializable
data class SignalBroadcastRequest(
    val signalName: String,
    val tenantId: String? = null,
)

@Serializable
data class ProcessInstanceItem(
    val processInstanceKey: String,
    val state: String,
    val processDefinitionKey: String? = null,
)

@Serializable
data class UserTaskItem(
    val userTaskKey: String,
    val name: String? = null,
    val state: String? = null,
    val assignee: String? = null,
    val elementId: String? = null,
    val processDefinitionId: String? = null,
    val processInstanceKey: String? = null,
    val candidateGroups: List<String> = emptyList(),
)

@Serializable
data class UserTaskSearchResponse(
    val items: List<UserTaskItem> = emptyList(),
    val page: CursorPage = CursorPage(),
)

@Serializable
data class UserTaskCompletionRequest(val variables: JsonObject)

@Serializable
data class DeployedProcess(
    val processDefinitionId: String,
    val processDefinitionKey: String,
    val processDefinitionVersion: Int = 0,
)

@Serializable
data class DeploymentMetadata(val processDefinition: DeployedProcess? = null)

@Serializable
data class DeploymentResponse(val deployments: List<DeploymentMetadata> = emptyList())

@Serializable
data class MappingInstruction(val sourceElementId: String, val targetElementId: String)

@Serializable
data class MigrationRequest(
    val targetProcessDefinitionKey: String,
    val mappingInstructions: List<MappingInstruction>,
)

@Serializable
data class CursorPage(val endCursor: String? = null)

@Serializable
data class ProcessInstanceSearchResponse(
    val items: List<ProcessInstanceItem> = emptyList(),
    val page: CursorPage = CursorPage(),
)

@Serializable
data class VariableItem(
    val processInstanceKey: String,
    val value: String? = null,
)

@Serializable
data class VariableSearchResponse(val items: List<VariableItem> = emptyList())

@Serializable
data class MessageSubscriptionItem(val correlationKey: String? = null)

@Serializable
data class MessageSubscriptionSearchResponse(val items: List<MessageSubscriptionItem> = emptyList())

@Serializable
data class SearchFilter(val processDefinitionId: String, val state: String? = null, val tenantId: String? = null)

@Serializable
data class SearchRequest(val filter: SearchFilter)

@Serializable
data class PageInfo(val totalItems: Long = 0)

@Serializable
data class SearchResponse(val page: PageInfo = PageInfo())
