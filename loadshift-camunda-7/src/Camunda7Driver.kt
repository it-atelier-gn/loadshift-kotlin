package loadshift.camunda7

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import loadshift.core.EngineApi
import loadshift.core.EngineDriver
import loadshift.core.EngineJob
import loadshift.core.EngineNames
import loadshift.core.RootInstance
import kotlin.time.Duration

@OptIn(EngineApi::class)
internal class Camunda7Driver(
    private val client: Camunda7Client,
    private val workerId: String,
) : EngineDriver {

    override fun pollGroups(jobTypes: List<String>): List<List<String>> = listOf(jobTypes)

    override suspend fun startInstance(processId: String, variables: JsonObject, businessKey: String?): String =
        client.startInstance(
            processId,
            CamundaVariables.toCamunda(variables),
            businessKey?.takeIf { it.length <= MAX_BUSINESS_KEY },
        ).id

    override suspend fun activeRoots(processId: String): List<RootInstance> {
        val instances = buildList {
            var first = 0
            while (true) {
                val page = client.processInstances(processId, first, PAGE_SIZE)
                addAll(page)
                if (page.size < PAGE_SIZE) break
                first += PAGE_SIZE
            }
        }
        val keys = instances.map { it.id }
            .chunked(QUERY_CHUNK)
            .flatMap { client.variableInstances(EngineNames.ITEM_KEY, it) }
            .mapNotNull { variable ->
                val instance = variable.processInstanceId ?: return@mapNotNull null
                instance to (variable.value as? JsonPrimitive)?.contentOrNull
            }
            .toMap()
        return instances.map { RootInstance(it.id, keys[it.id]?.takeIf { key -> key.isNotEmpty() }) }
    }

    override suspend fun fetch(jobTypes: List<String>, maxJobs: Int, lock: Duration, wait: Duration): List<EngineJob> =
        client.fetchAndLock(
            FetchAndLockRequest(
                workerId = workerId,
                maxTasks = maxJobs,
                asyncResponseTimeout = wait.inWholeMilliseconds.takeIf { it > 0 },
                topics = jobTypes.map { FetchTopicDto(topicName = it, lockDuration = lock.inWholeMilliseconds) },
            ),
        ).map { task ->
            EngineJob(
                id = task.id,
                type = task.topicName,
                instanceId = task.processInstanceId,
                variables = CamundaVariables.toJsonElement(CamundaVariables.fromCamunda(task.variables)) as JsonObject,
                retries = task.retries,
            )
        }

    override suspend fun complete(job: EngineJob, variables: JsonObject) {
        client.complete(job.id, CompleteRequest(workerId, CamundaVariables.toCamunda(variables)))
    }

    override suspend fun fail(job: EngineJob, retries: Int, backoff: Duration, message: String, details: String) {
        client.failure(
            job.id,
            FailureRequest(
                workerId = workerId,
                errorMessage = message.take(MAX_ERROR_MESSAGE),
                errorDetails = details,
                retries = retries,
                retryTimeout = backoff.inWholeMilliseconds,
            ),
        )
    }

    override suspend fun throwError(job: EngineJob, errorCode: String, message: String, variables: JsonObject) {
        client.bpmnError(
            job.id,
            BpmnErrorRequest(
                workerId = workerId,
                errorCode = errorCode,
                errorMessage = message.take(MAX_ERROR_MESSAGE),
                variables = CamundaVariables.toCamunda(variables),
            ),
        )
    }

    override suspend fun extendLock(job: EngineJob, lock: Duration) {
        client.extendLock(job.id, ExtendLockRequest(workerId, lock.inWholeMilliseconds))
    }

    override suspend fun release(job: EngineJob) {
        client.unlock(job.id)
    }

    override suspend fun finished(instanceIds: List<String>): Map<String, Boolean> =
        instanceIds.chunked(QUERY_CHUNK)
            .flatMap { client.historicProcessInstances(it) }
            .associate { it.id to (it.state == "COMPLETED") }

    override suspend fun cancel(instanceId: String) {
        client.deleteProcessInstance(instanceId)
    }

    override suspend fun correlate(message: String, workflowKey: String, itemKey: String?, variables: JsonObject): Boolean {
        val keys = buildMap {
            put(EngineNames.WORKFLOW, CamundaVariables.encode(workflowKey))
            if (itemKey != null) put(EngineNames.ITEM_KEY, CamundaVariables.encode(itemKey))
        }
        val request = MessageRequest(
            message,
            keys,
            all = true,
            resultEnabled = true,
            processVariables = variables.takeIf { it.isNotEmpty() }?.let(CamundaVariables::toCamunda),
        )
        return client.correlateMessage(request) > 0
    }

    override suspend fun activeInstances(processIds: List<String>): Long =
        processIds.sumOf { client.processInstanceCount(it) }

    internal companion object {
        const val MAX_BUSINESS_KEY = 255
        const val MAX_ERROR_MESSAGE = 666
        const val QUERY_CHUNK = 100
        const val PAGE_SIZE = 500
    }
}
