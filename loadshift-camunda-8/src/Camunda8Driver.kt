package loadshift.camunda8

import kotlinx.serialization.json.Json
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
internal class Camunda8Driver(
    private val client: Camunda8Client,
    private val workerId: String,
) : EngineDriver {

    override fun pollGroups(jobTypes: List<String>): List<List<String>> = jobTypes.map { listOf(it) }

    override suspend fun startInstance(processId: String, variables: JsonObject, businessKey: String?): String =
        client.createInstance(processId, variables).processInstanceKey

    override suspend fun activeRoots(processId: String): List<RootInstance> {
        val keys = buildList {
            var cursor: String? = null
            while (true) {
                val page = client.activeInstances(processId, cursor, PAGE_SIZE)
                page.items.mapTo(this) { it.processInstanceKey }
                cursor = page.page.endCursor
                if (page.items.isEmpty() || cursor == null) break
            }
        }.distinct()
        val itemKeys = keys.chunked(QUERY_CHUNK)
            .flatMap { client.variables(EngineNames.ITEM_KEY, it) }
            .associate { variable ->
                val decoded = variable.value?.let { (Json.parseToJsonElement(it) as? JsonPrimitive)?.contentOrNull }
                variable.processInstanceKey to decoded
            }
        return keys.map { RootInstance(it, itemKeys[it]?.takeIf { key -> key.isNotEmpty() }) }
    }

    override suspend fun fetch(jobTypes: List<String>, maxJobs: Int, lock: Duration, wait: Duration): List<EngineJob> =
        jobTypes.flatMap { type ->
            client.activateJobs(
                ActivateJobsRequest(
                    type = type,
                    worker = workerId,
                    timeout = lock.inWholeMilliseconds,
                    maxJobsToActivate = maxJobs,
                    requestTimeout = if (wait.isPositive()) wait.inWholeMilliseconds else NO_LONG_POLLING,
                ),
            ).map { job ->
                EngineJob(
                    id = job.jobKey,
                    type = type,
                    instanceId = job.processInstanceKey,
                    variables = job.variables,
                    retries = job.retries,
                )
            }
        }

    override suspend fun complete(job: EngineJob, variables: JsonObject) {
        client.completeJob(job.id, CompleteJobRequest(variables))
    }

    override suspend fun fail(job: EngineJob, retries: Int, backoff: Duration, message: String, details: String) {
        client.failJob(job.id, FailJobRequest(retries, message, backoff.inWholeMilliseconds))
    }

    override suspend fun terminate(job: EngineJob, message: String, variables: JsonObject) {
        client.throwError(job.id, JobErrorRequest(EngineNames.TERMINATE_ERROR, message, variables))
    }

    override suspend fun extendLock(job: EngineJob, lock: Duration) {
        client.updateJob(job.id, JobUpdateRequest(JobChangeset(timeout = lock.inWholeMilliseconds)))
    }

    override suspend fun release(job: EngineJob) {
        client.updateJob(job.id, JobUpdateRequest(JobChangeset(timeout = RELEASE_TIMEOUT_MILLIS)))
    }

    override suspend fun finished(instanceIds: List<String>): Map<String, Boolean> =
        instanceIds.chunked(QUERY_CHUNK)
            .flatMap { client.searchProcessInstances(it) }
            .filter { it.state != "ACTIVE" }
            .associate { it.processInstanceKey to (it.state == "COMPLETED") }

    override suspend fun cancel(instanceId: String) {
        client.cancelInstance(instanceId)
    }

    override suspend fun correlate(message: String, workflowKey: String, itemKey: String?): Boolean {
        if (itemKey != null) return client.correlateMessage(message, EngineNames.correlationKey(workflowKey, itemKey))
        val prefix = EngineNames.correlationKey(workflowKey, "")
        val keys = client.messageSubscriptions(message)
            .mapNotNull { it.correlationKey }
            .filter { it.startsWith(prefix) }
            .toSet()
        var correlated = false
        for (key in keys) if (client.correlateMessage(message, key)) correlated = true
        return correlated
    }

    override suspend fun activeInstances(processIds: List<String>): Long =
        processIds.sumOf { client.instanceCount(it) }

    private companion object {
        const val NO_LONG_POLLING = -1L
        const val RELEASE_TIMEOUT_MILLIS = 1L
        const val QUERY_CHUNK = 100
        const val PAGE_SIZE = 100
    }
}
