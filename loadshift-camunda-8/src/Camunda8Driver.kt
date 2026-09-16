package loadshift.camunda8

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
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
    private val jobStream: Camunda8JobStream? = null,
    private val tenantId: String? = null,
    private val streamReopenDelay: Duration = 5.seconds,
) : EngineDriver {
    private class OpenStream {
        @Volatile var handle: AutoCloseable? = null
    }

    private val streamed = ConcurrentHashMap<String, Channel<EngineJob>>()
    private val streams = ConcurrentHashMap<String, OpenStream>()
    private val reopenAt = ConcurrentHashMap<String, TimeSource.Monotonic.ValueTimeMark>()
    private val closed = AtomicBoolean(false)

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

    override suspend fun fetch(jobTypes: List<String>, maxJobs: Int, lock: Duration, wait: Duration): List<EngineJob> {
        val stream = jobStream ?: return activate(jobTypes, maxJobs, lock, wait)
        if (closed.get()) return emptyList()
        val channels = jobTypes.map { type ->
            openStream(stream, type, lock)
            channel(type)
        }
        drain(channels, maxJobs).takeIf { it.isNotEmpty() }?.let { return it }
        activate(jobTypes, maxJobs, lock, Duration.ZERO).takeIf { it.isNotEmpty() }?.let { return it }
        val first = withTimeoutOrNull(wait.coerceAtLeast(MIN_STREAM_WAIT)) {
            select<EngineJob> { channels.forEach { channel -> channel.onReceive { it } } }
        } ?: return emptyList()
        return listOf(first) + drain(channels, maxJobs - 1)
    }

    override suspend fun close() {
        if (!closed.compareAndSet(false, true)) return
        for (stream in streams.values) runCatching { stream.handle?.close() }
        streams.clear()
        for (channel in streamed.values) {
            channel.close()
            while (true) {
                val job = channel.tryReceive().getOrNull() ?: break
                runCatching { release(job) }
            }
        }
    }

    private fun channel(type: String): Channel<EngineJob> = streamed.computeIfAbsent(type) { Channel(STREAM_BUFFER) }

    private fun openStream(stream: Camunda8JobStream, type: String, lock: Duration) {
        if (closed.get() || streams.containsKey(type)) return
        if (reopenAt[type]?.hasNotPassedNow() == true) return
        val opened = OpenStream()
        if (streams.putIfAbsent(type, opened) != null) return
        val channel = channel(type)
        val handle = try {
            stream.open(
                type,
                workerId,
                lock,
                tenantId,
                { job ->
                    val engineJob = EngineJob(job.jobKey, job.type, job.processInstanceKey, job.variables, job.retries)
                    runCatching { runBlocking { channel.send(engineJob) } }
                },
                { _ ->
                    if (streams.remove(type, opened)) reopenAt[type] = TimeSource.Monotonic.markNow() + streamReopenDelay
                },
            )
        } catch (e: Exception) {
            streams.remove(type, opened)
            reopenAt[type] = TimeSource.Monotonic.markNow() + streamReopenDelay
            return
        }
        opened.handle = handle
        if (streams[type] !== opened || closed.get()) runCatching { handle.close() }
    }

    private fun drain(channels: List<Channel<EngineJob>>, maxJobs: Int): List<EngineJob> = buildList {
        for (channel in channels) {
            while (size < maxJobs) add(channel.tryReceive().getOrNull() ?: break)
        }
    }

    private suspend fun activate(jobTypes: List<String>, maxJobs: Int, lock: Duration, wait: Duration): List<EngineJob> =
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

    override suspend fun throwError(job: EngineJob, errorCode: String, message: String, variables: JsonObject) {
        client.throwError(job.id, JobErrorRequest(errorCode, message, variables))
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

    override suspend fun correlate(message: String, workflowKey: String, itemKey: String?, variables: JsonObject): Boolean {
        val payload = variables.takeIf { it.isNotEmpty() }
        if (itemKey != null) return client.correlateMessage(message, EngineNames.correlationKey(workflowKey, itemKey), payload)
        val prefix = EngineNames.correlationKey(workflowKey, "")
        val keys = client.messageSubscriptions(message)
            .mapNotNull { it.correlationKey }
            .filter { it.startsWith(prefix) }
            .toSet()
        var correlated = false
        for (key in keys) if (client.correlateMessage(message, key, payload)) correlated = true
        return correlated
    }

    override suspend fun activeInstances(processIds: List<String>): Long =
        processIds.sumOf { client.instanceCount(it) }

    private companion object {
        const val NO_LONG_POLLING = -1L
        const val STREAM_BUFFER = 32
        val MIN_STREAM_WAIT = 10.milliseconds
        const val RELEASE_TIMEOUT_MILLIS = 1L
        const val QUERY_CHUNK = 100
        const val PAGE_SIZE = 100
    }
}
