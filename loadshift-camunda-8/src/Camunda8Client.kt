package loadshift.camunda8

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.forms.MultiPartFormDataContent
import io.ktor.client.request.forms.formData
import io.ktor.client.request.header
import io.ktor.client.request.patch
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.Headers
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject

class Camunda8Client(
    base: String = "http://localhost:8080",
    private val token: String? = null,
) {
    private val v2 = "$base/v2"

    private val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
        encodeDefaults = true
    }

    private val http = HttpClient(CIO) {
        install(ContentNegotiation) { json(json) }
    }

    suspend fun deploy(resources: List<Pair<String, ByteArray>>) {
        val response = http.post("$v2/deployments") {
            auth()
            setBody(
                MultiPartFormDataContent(
                    formData {
                        for ((fileName, bytes) in resources) {
                            append(
                                "resources",
                                bytes,
                                Headers.build {
                                    append(HttpHeaders.ContentType, ContentType.Application.OctetStream.toString())
                                    append(HttpHeaders.ContentDisposition, "filename=\"$fileName\"")
                                },
                            )
                        }
                    },
                ),
            )
        }
        response.ensureSuccess("deploy")
    }

    suspend fun createInstance(processDefinitionId: String, variables: JsonObject): CreateInstanceResponse {
        val response = postJson("$v2/process-instances", CreateInstanceRequest(processDefinitionId, variables))
        response.ensureSuccess("createInstance")
        return response.body()
    }

    suspend fun activateJobs(request: ActivateJobsRequest): List<ActivatedJob> {
        val response = postJson("$v2/jobs/activation", request)
        response.ensureSuccess("activateJobs")
        return response.body<ActivateJobsResponse>().jobs
    }

    suspend fun completeJob(jobKey: String, request: CompleteJobRequest) {
        postJson("$v2/jobs/$jobKey/completion", request).ensureSuccess("completeJob")
    }

    suspend fun failJob(jobKey: String, request: FailJobRequest) {
        postJson("$v2/jobs/$jobKey/failure", request).ensureSuccess("failJob")
    }

    suspend fun throwError(jobKey: String, request: JobErrorRequest) {
        postJson("$v2/jobs/$jobKey/error", request).ensureSuccess("throwError")
    }

    suspend fun updateJob(jobKey: String, request: JobUpdateRequest) {
        val response = http.patch("$v2/jobs/$jobKey") {
            auth()
            contentType(ContentType.Application.Json)
            setBody(request)
        }
        response.ensureSuccess("updateJob")
    }

    suspend fun searchProcessInstances(processInstanceKeys: List<String>): List<ProcessInstanceItem> {
        if (processInstanceKeys.isEmpty()) return emptyList()
        val query = buildJsonObject {
            putJsonObject("filter") {
                putJsonObject("processInstanceKey") {
                    putJsonArray("\$in") { processInstanceKeys.forEach { add(it) } }
                }
            }
            putJsonObject("page") { put("limit", processInstanceKeys.size) }
        }
        val response = postJson("$v2/process-instances/search", query)
        response.ensureSuccess("searchProcessInstances")
        return response.body<ProcessInstanceSearchResponse>().items
    }

    suspend fun cancelInstance(processInstanceKey: String) {
        val response = postJson("$v2/process-instances/$processInstanceKey/cancellation", JsonObject(emptyMap()))
        if (response.status != HttpStatusCode.NotFound) response.ensureSuccess("cancelInstance")
    }

    suspend fun correlateMessage(name: String, correlationKey: String): Boolean =
        postJson("$v2/messages/correlation", MessageCorrelationRequest(name, correlationKey)).status.isSuccess()

    suspend fun messageSubscriptions(messageName: String): List<MessageSubscriptionItem> {
        val query = buildJsonObject {
            putJsonObject("filter") {
                put("messageName", messageName)
                put("messageSubscriptionState", "CREATED")
            }
            putJsonObject("page") { put("limit", SUBSCRIPTION_PAGE) }
        }
        val response = postJson("$v2/message-subscriptions/search", query)
        response.ensureSuccess("messageSubscriptions")
        return response.body<MessageSubscriptionSearchResponse>().items
    }

    suspend fun instanceCount(processDefinitionId: String): Long {
        val response = postJson("$v2/process-instances/search", SearchRequest(SearchFilter(processDefinitionId, state = "ACTIVE")))
        response.ensureSuccess("instanceCount")
        return response.body<SearchResponse>().page.totalItems
    }

    fun close() = http.close()

    private fun HttpRequestBuilder.auth() {
        token?.let { header(HttpHeaders.Authorization, "Bearer $it") }
    }

    private suspend inline fun <reified T> postJson(url: String, body: T): HttpResponse =
        http.post(url) {
            auth()
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun HttpResponse.ensureSuccess(operation: String) {
        if (!status.isSuccess()) error("$operation failed: $status ${bodyAsText()}")
    }

    private companion object {
        const val SUBSCRIPTION_PAGE = 1000
    }
}
