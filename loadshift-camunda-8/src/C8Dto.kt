package loadshift.camunda8

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

@Serializable
data class CreateInstanceRequest(
    val processDefinitionId: String,
    val variables: JsonObject,
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
)

@Serializable
data class ProcessInstanceItem(
    val processInstanceKey: String,
    val state: String,
)

@Serializable
data class ProcessInstanceSearchResponse(val items: List<ProcessInstanceItem> = emptyList())

@Serializable
data class MessageSubscriptionItem(val correlationKey: String? = null)

@Serializable
data class MessageSubscriptionSearchResponse(val items: List<MessageSubscriptionItem> = emptyList())

@Serializable
data class SearchFilter(val processDefinitionId: String, val state: String? = null)

@Serializable
data class SearchRequest(val filter: SearchFilter)

@Serializable
data class PageInfo(val totalItems: Long = 0)

@Serializable
data class SearchResponse(val page: PageInfo = PageInfo())
