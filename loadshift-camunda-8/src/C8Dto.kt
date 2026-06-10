package loadshift.camunda8

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

@Serializable
data class CreateInstanceRequest(
    val processDefinitionId: String,
    val variables: JsonObject,
)

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
    val processInstanceKey: String? = null,
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
data class SearchFilter(val processDefinitionId: String)

@Serializable
data class SearchRequest(val filter: SearchFilter)

@Serializable
data class PageInfo(val totalItems: Long = 0)

@Serializable
data class SearchResponse(val page: PageInfo = PageInfo())
