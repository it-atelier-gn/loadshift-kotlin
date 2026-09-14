package loadshift.camunda8.stream

import io.camunda.client.CamundaClient
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import loadshift.camunda8.Camunda8JobStream
import loadshift.camunda8.StreamedJob
import java.net.URI
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.toJavaDuration

class CamundaJobStream private constructor(
    private val client: CamundaClient,
    private val ownsClient: Boolean,
) : Camunda8JobStream, AutoCloseable {

    constructor(client: CamundaClient) : this(client, ownsClient = false)

    constructor(grpcAddress: URI, restAddress: URI) : this(
        CamundaClient.newClientBuilder().grpcAddress(grpcAddress).restAddress(restAddress).preferRestOverGrpc(false).build(),
        ownsClient = true,
    )

    override fun open(
        jobType: String,
        worker: String,
        timeout: Duration,
        tenantId: String?,
        receive: (StreamedJob) -> Unit,
        ended: (Throwable?) -> Unit,
    ): AutoCloseable {
        val command = client.newStreamJobsCommand()
            .jobType(jobType)
            .consumer { job ->
                val variables = job.variables?.takeIf { it.isNotBlank() }?.let { Json.parseToJsonElement(it).jsonObject }
                receive(StreamedJob(job.key.toString(), job.type, job.processInstanceKey.toString(), job.retries, variables ?: JsonObject(emptyMap())))
            }
            .workerName(worker)
            .timeout(timeout.toJavaDuration())
        val future = (if (tenantId == null) command else command.tenantId(tenantId)).send()
        val cancelled = AtomicBoolean(false)
        future.whenComplete { _, error -> if (!cancelled.get()) ended(error) }
        return AutoCloseable { if (cancelled.compareAndSet(false, true)) future.cancel(true) }
    }

    override fun close() {
        if (ownsClient) client.close()
    }
}
