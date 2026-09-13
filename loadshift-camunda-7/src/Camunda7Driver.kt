package loadshift.camunda7

import kotlinx.serialization.json.JsonObject
import loadshift.core.EngineApi
import loadshift.core.EngineDriver
import loadshift.core.EngineJob
import loadshift.core.EngineNames
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

    override suspend fun terminate(job: EngineJob, message: String) {
        client.bpmnError(job.id, BpmnErrorRequest(workerId, EngineNames.TERMINATE_ERROR, message.take(MAX_ERROR_MESSAGE)))
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

    override suspend fun correlate(message: String, runId: String, itemKey: String?): Boolean {
        val keys = buildMap {
            put(EngineNames.RUN_ID, CamundaVariables.encode(runId))
            if (itemKey != null) put(EngineNames.ITEM_KEY, CamundaVariables.encode(itemKey))
        }
        return client.correlateMessage(MessageRequest(message, keys, all = true, resultEnabled = true)) > 0
    }

    override suspend fun activeInstances(processIds: List<String>): Long =
        processIds.sumOf { client.processInstanceCount(it) }

    private companion object {
        const val MAX_BUSINESS_KEY = 255
        const val MAX_ERROR_MESSAGE = 666
        const val QUERY_CHUNK = 100
    }
}
