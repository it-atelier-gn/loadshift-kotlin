package loadshift.camunda7

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.delete
import io.ktor.client.request.forms.MultiPartFormDataContent
import io.ktor.client.request.forms.formData
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.parameter
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
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import java.util.Base64

class BasicCredentials(val username: String, val password: String) {
    internal fun header(): String =
        "Basic " + Base64.getEncoder().encodeToString("$username:$password".toByteArray(Charsets.UTF_8))

    override fun toString(): String = "BasicCredentials(username=$username)"
}

class Camunda7Client internal constructor(
    private val base: String,
    credentials: BasicCredentials?,
    private val engine: HttpClientEngine,
    private val tenantId: String? = null,
) {
    constructor(
        base: String = "http://localhost:8080/engine-rest",
        credentials: BasicCredentials? = null,
        tenantId: String? = null,
    ) : this(base, credentials, CIO.create(), tenantId)

    private val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
        encodeDefaults = true
    }

    private val http = HttpClient(engine) {
        install(ContentNegotiation) { json(json) }
        if (credentials != null) {
            val authorization = credentials.header()
            defaultRequest { header(HttpHeaders.Authorization, authorization) }
        }
    }

    suspend fun deploy(name: String, resources: List<Pair<String, ByteArray>>): DeploymentDto {
        val response = http.post("$base/deployment/create") {
            setBody(
                MultiPartFormDataContent(
                    formData {
                        append("deployment-name", name)
                        append("enable-duplicate-filtering", "true")
                        append("deploy-changed-only", "true")
                        tenantId?.let { append("tenant-id", it) }
                        for ((fileName, bytes) in resources) {
                            append(
                                fileName,
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
        return response.body()
    }

    suspend fun startInstance(
        processDefinitionKey: String,
        variables: Map<String, CamundaValue>,
        businessKey: String?,
    ): StartInstanceResponse {
        val definition = tenantId?.let { "$processDefinitionKey/tenant-id/$it" } ?: processDefinitionKey
        val response = postJson("$base/process-definition/key/$definition/start", StartInstanceRequest(variables, businessKey))
        response.ensureSuccess("start")
        return response.body()
    }

    suspend fun processInstances(processDefinitionKey: String, firstResult: Int, maxResults: Int): List<ProcessInstanceDto> {
        val response = http.get("$base/process-instance") {
            parameter("processDefinitionKey", processDefinitionKey)
            tenantId?.let { parameter("tenantIdIn", it) }
            parameter("sortBy", "instanceId")
            parameter("sortOrder", "asc")
            parameter("firstResult", firstResult)
            parameter("maxResults", maxResults)
        }
        response.ensureSuccess("list process instances")
        return response.body()
    }

    suspend fun variableInstances(variableName: String, processInstanceIds: List<String>): List<VariableInstanceDto> {
        if (processInstanceIds.isEmpty()) return emptyList()
        val response = http.post("$base/variable-instance") {
            parameter("maxResults", processInstanceIds.size)
            parameter("deserializeValues", false)
            contentType(ContentType.Application.Json)
            setBody(VariableInstanceQuery(variableName, processInstanceIds))
        }
        response.ensureSuccess("variable query")
        return response.body()
    }

    suspend fun fetchAndLock(request: FetchAndLockRequest): List<ExternalTaskDto> {
        val scoped = tenantId?.let { tenant -> request.copy(topics = request.topics.map { it.copy(tenantIdIn = listOf(tenant)) }) }
        val response = postJson("$base/external-task/fetchAndLock", scoped ?: request)
        response.ensureSuccess("fetchAndLock")
        return response.body()
    }

    suspend fun complete(taskId: String, request: CompleteRequest) {
        postJson("$base/external-task/$taskId/complete", request).ensureSuccess("complete")
    }

    suspend fun failure(taskId: String, request: FailureRequest) {
        postJson("$base/external-task/$taskId/failure", request).ensureSuccess("failure")
    }

    suspend fun bpmnError(taskId: String, request: BpmnErrorRequest) {
        postJson("$base/external-task/$taskId/bpmnError", request).ensureSuccess("bpmnError")
    }

    suspend fun unlock(taskId: String) {
        val response = http.post("$base/external-task/$taskId/unlock")
        if (response.status != HttpStatusCode.NotFound) response.ensureSuccess("unlock")
    }

    suspend fun extendLock(taskId: String, request: ExtendLockRequest) {
        postJson("$base/external-task/$taskId/extendLock", request).ensureSuccess("extendLock")
    }

    suspend fun processInstanceCount(processDefinitionKey: String): Long {
        val response = http.get("$base/process-instance/count") {
            parameter("processDefinitionKey", processDefinitionKey)
            tenantId?.let { parameter("tenantIdIn", it) }
        }
        response.ensureSuccess("process instance count")
        return response.body<CountDto>().count
    }

    suspend fun historicProcessInstances(processInstanceIds: List<String>): List<HistoricProcessInstanceDto> {
        if (processInstanceIds.isEmpty()) return emptyList()
        val response = http.post("$base/history/process-instance") {
            parameter("maxResults", processInstanceIds.size)
            contentType(ContentType.Application.Json)
            setBody(HistoricProcessInstanceQuery(processInstanceIds))
        }
        response.ensureSuccess("history query")
        return response.body()
    }

    suspend fun deleteProcessInstance(processInstanceId: String) {
        val response = http.delete("$base/process-instance/$processInstanceId") {
            parameter("skipCustomListeners", true)
        }
        if (response.status != HttpStatusCode.NotFound) response.ensureSuccess("delete process instance")
    }

    suspend fun correlateMessage(request: MessageRequest): Int {
        val response = postJson("$base/message", tenantId?.let { request.copy(tenantId = it) } ?: request)
        if (!response.status.isSuccess()) return 0
        return if (request.resultEnabled) response.body<JsonArray>().size else 1
    }

    suspend fun tasks(processDefinitionKey: String, firstResult: Int, maxResults: Int): List<TaskDto> {
        val response = http.get("$base/task") {
            parameter("processDefinitionKey", processDefinitionKey)
            tenantId?.let { parameter("tenantIdIn", it) }
            parameter("sortBy", "id")
            parameter("sortOrder", "asc")
            parameter("firstResult", firstResult)
            parameter("maxResults", maxResults)
        }
        response.ensureSuccess("task query")
        return response.body()
    }

    suspend fun task(taskId: String): TaskDto? {
        val response = http.get("$base/task/$taskId")
        if (response.status == HttpStatusCode.NotFound) return null
        response.ensureSuccess("task")
        return response.body()
    }

    suspend fun completeTask(taskId: String, variables: Map<String, CamundaValue>): Boolean {
        val response = postJson("$base/task/$taskId/complete", CompleteTaskRequest(variables))
        if (response.status == HttpStatusCode.NotFound) return false
        response.ensureSuccess("complete task")
        return true
    }

    suspend fun latestProcessDefinition(processDefinitionKey: String): ProcessDefinitionDto? {
        val response = http.get("$base/process-definition") {
            parameter("key", processDefinitionKey)
            parameter("latestVersion", true)
            if (tenantId == null) parameter("withoutTenantId", true) else parameter("tenantIdIn", tenantId)
        }
        response.ensureSuccess("process definition query")
        return response.body<List<ProcessDefinitionDto>>().firstOrNull()
    }

    suspend fun generateMigration(sourceProcessDefinitionId: String, targetProcessDefinitionId: String): JsonObject {
        val response = postJson(
            "$base/migration/generate",
            MigrationGenerateRequest(sourceProcessDefinitionId, targetProcessDefinitionId),
        )
        response.ensureSuccess("generate migration")
        return response.body()
    }

    suspend fun executeMigration(plan: JsonObject, processInstanceIds: List<String>) {
        postJson("$base/migration/execute", MigrationExecuteRequest(plan, processInstanceIds)).ensureSuccess("execute migration")
    }

    suspend fun signal(name: String) {
        postJson("$base/signal", SignalRequest(name, tenantId)).ensureSuccess("signal")
    }

    fun close() {
        http.close()
        engine.close()
    }

    private suspend inline fun <reified T> postJson(url: String, body: T): HttpResponse =
        http.post(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun HttpResponse.ensureSuccess(operation: String) {
        if (!status.isSuccess()) error("$operation failed: $status ${bodyAsText()}")
    }
}
