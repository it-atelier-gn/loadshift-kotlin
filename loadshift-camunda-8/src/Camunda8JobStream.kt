package loadshift.camunda8

import kotlinx.serialization.json.JsonObject
import kotlin.time.Duration

class StreamedJob(
    val jobKey: String,
    val type: String,
    val processInstanceKey: String,
    val retries: Int?,
    val variables: JsonObject,
)

interface Camunda8JobStream {
    fun open(
        jobType: String,
        worker: String,
        timeout: Duration,
        tenantId: String?,
        receive: (StreamedJob) -> Unit,
        ended: (Throwable?) -> Unit,
    ): AutoCloseable
}
