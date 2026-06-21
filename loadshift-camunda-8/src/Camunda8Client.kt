package loadshift.camunda8

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.forms.MultiPartFormDataContent
import io.ktor.client.request.forms.formData
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.Headers
import io.ktor.http.HttpHeaders
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject

class Camunda8Client(
    base: String = "http://localhost:8080",
    private val token: String? = null,
) {
    private val v2 = "$base/v2"

    private val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
    }

    private val http = HttpClient(CIO) {
        install(ContentNegotiation) { json(json) }
    }

    private fun io.ktor.client.request.HttpRequestBuilder.auth() {
        token?.let { header(HttpHeaders.Authorization, "Bearer $it") }
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
        if (!response.status.isSuccess()) error("deploy failed: ${response.status} ${response.bodyAsText()}")
    }

    suspend fun createInstance(processDefinitionId: String, variables: JsonObject) {
        val response = http.post("$v2/process-instances") {
            auth()
            contentType(ContentType.Application.Json)
            setBody(CreateInstanceRequest(processDefinitionId, variables))
        }
        if (!response.status.isSuccess()) error("createInstance failed: ${response.status} ${response.bodyAsText()}")
    }

    suspend fun publishMessage(name: String, correlationKey: String) {
        runCatching {
            http.post("$v2/messages/publication") {
                auth()
                contentType(ContentType.Application.Json)
                setBody(PublishMessageRequest(name, correlationKey))
            }
        }
    }

    suspend fun activateJobs(request: ActivateJobsRequest): List<ActivatedJob> {
        val response = http.post("$v2/jobs/activation") {
            auth()
            contentType(ContentType.Application.Json)
            setBody(request)
        }
        if (!response.status.isSuccess()) return emptyList()
        return response.body<ActivateJobsResponse>().jobs
    }

    suspend fun completeJob(jobKey: String, request: CompleteJobRequest) {
        val response = http.post("$v2/jobs/$jobKey/completion") {
            auth()
            contentType(ContentType.Application.Json)
            setBody(request)
        }
        if (!response.status.isSuccess()) error("completeJob failed: ${response.status} ${response.bodyAsText()}")
    }

    suspend fun failJob(jobKey: String, request: FailJobRequest) {
        val response = http.post("$v2/jobs/$jobKey/failure") {
            auth()
            contentType(ContentType.Application.Json)
            setBody(request)
        }
        if (!response.status.isSuccess()) error("failJob failed: ${response.status} ${response.bodyAsText()}")
    }

    suspend fun instanceCount(processDefinitionId: String): Long {
        val response = http.post("$v2/process-instances/search") {
            auth()
            contentType(ContentType.Application.Json)
            setBody(SearchRequest(SearchFilter(processDefinitionId, state = "ACTIVE")))
        }
        if (!response.status.isSuccess()) return 0
        return response.body<SearchResponse>().page.totalItems
    }

    fun close() = http.close()
}
