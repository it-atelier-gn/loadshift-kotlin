package loadshift.camunda7

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.forms.MultiPartFormDataContent
import io.ktor.client.request.forms.formData
import io.ktor.client.request.get
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

class Camunda7Client(
    private val base: String = "http://localhost:8080/engine-rest",
) {
    private val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
    }

    private val http = HttpClient(CIO) {
        install(ContentNegotiation) { json(json) }
    }

    suspend fun deploy(name: String, resources: List<Pair<String, ByteArray>>): DeploymentDto {
        val response = http.post("$base/deployment/create") {
            setBody(
                MultiPartFormDataContent(
                    formData {
                        append("deployment-name", name)
                        append("enable-duplicate-filtering", "true")
                        append("deploy-changed-only", "true")
                        for ((fileName, bytes) in resources) {
                            append(
                                "data",
                                bytes,
                                Headers.build {
                                    append(HttpHeaders.ContentType, ContentType.Application.OctetStream.toString())
                                    append(HttpHeaders.ContentDisposition, "form-data; name=\"data\"; filename=\"$fileName\"")
                                },
                            )
                        }
                    },
                ),
            )
        }
        if (!response.status.isSuccess()) error("deploy failed: ${response.status} ${response.bodyAsText()}")
        return response.body()
    }

    suspend fun startInstance(
        processDefinitionKey: String,
        variables: Map<String, CamundaValue>,
        businessKey: String?,
    ): StartInstanceResponse {
        val response = http.post("$base/process-definition/key/$processDefinitionKey/start") {
            contentType(ContentType.Application.Json)
            setBody(StartInstanceRequest(variables, businessKey))
        }
        if (!response.status.isSuccess()) error("start failed: ${response.status} ${response.bodyAsText()}")
        return response.body()
    }

    suspend fun fetchAndLock(request: FetchAndLockRequest): List<ExternalTaskDto> {
        val response = http.post("$base/external-task/fetchAndLock") {
            contentType(ContentType.Application.Json)
            setBody(request)
        }
        if (!response.status.isSuccess()) error("fetchAndLock failed: ${response.status} ${response.bodyAsText()}")
        return response.body()
    }

    suspend fun complete(taskId: String, request: CompleteRequest) {
        val response = http.post("$base/external-task/$taskId/complete") {
            contentType(ContentType.Application.Json)
            setBody(request)
        }
        if (!response.status.isSuccess()) error("complete failed: ${response.status} ${response.bodyAsText()}")
    }

    suspend fun failure(taskId: String, request: FailureRequest) {
        val response = http.post("$base/external-task/$taskId/failure") {
            contentType(ContentType.Application.Json)
            setBody(request)
        }
        if (!response.status.isSuccess()) error("failure failed: ${response.status} ${response.bodyAsText()}")
    }

    suspend fun extendLock(taskId: String, request: ExtendLockRequest) {
        val response = http.post("$base/external-task/$taskId/extendLock") {
            contentType(ContentType.Application.Json)
            setBody(request)
        }
        if (!response.status.isSuccess()) error("extendLock failed: ${response.status} ${response.bodyAsText()}")
    }

    suspend fun processInstanceCount(processDefinitionKey: String): Long {
        val response = http.get("$base/process-instance/count") {
            url.parameters.append("processDefinitionKey", processDefinitionKey)
        }
        if (!response.status.isSuccess()) return 0
        return response.body<CountDto>().count
    }

    fun close() = http.close()
}
