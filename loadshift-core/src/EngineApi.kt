package loadshift.core

import kotlinx.serialization.json.JsonObject
import kotlin.time.Duration

@RequiresOptIn(
    message = "Building block for engine backends. Workflows use Backend and RunHandle.",
    level = RequiresOptIn.Level.ERROR,
)
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY)
annotation class EngineApi

object EngineNames {
    const val ITEM_KEY = "loadshiftKey"
    const val RUN_ID = "loadshiftRun"
    const val PARENTS = "loadshiftParents"
    const val TERMINATE_ERROR = "loadshift-terminate"

    fun jobType(workflowKey: String, name: String): String = "$workflowKey/$name"
    fun decision(stepId: String): String = "decision_$stepId"
    fun expand(stepId: String): String = "expand_$stepId"
    fun reduce(stepId: String): String = "reduce_$stepId"
    fun timeout(stepId: String): String = "timeout_$stepId"
    fun loop(stepId: String): String = "loop_$stepId"
    fun compensate(topic: String): String = "compensate_$topic"
    fun items(stepId: String): String = "${stepId}_items"
    fun item(stepId: String): String = "${stepId}_item"
    fun result(stepId: String): String = "${stepId}_result"
    fun iterations(stepId: String): String = "${stepId}_iterations"
    fun correlationKey(runId: String, itemKey: String): String = "$runId:$itemKey"
}

@EngineApi
class EngineJob(
    val id: String,
    val type: String,
    val instanceId: String,
    val variables: JsonObject,
    val retries: Int?,
)

@EngineApi
sealed interface JobOutcome {
    data class Complete(val variables: JsonObject) : JobOutcome
    data class Retry(val retries: Int, val backoff: Duration, val message: String, val details: String) : JobOutcome
    data class Terminate(val message: String) : JobOutcome
    data class Abort(val cause: Throwable) : JobOutcome
}

@EngineApi
interface EngineDriver {
    fun pollGroups(jobTypes: List<String>): List<List<String>>
    suspend fun startInstance(processId: String, variables: JsonObject, businessKey: String?): String
    suspend fun fetch(jobTypes: List<String>, maxJobs: Int, lock: Duration, wait: Duration): List<EngineJob>
    suspend fun complete(job: EngineJob, variables: JsonObject)
    suspend fun fail(job: EngineJob, retries: Int, backoff: Duration, message: String, details: String)
    suspend fun terminate(job: EngineJob, message: String)
    suspend fun extendLock(job: EngineJob, lock: Duration)
    suspend fun release(job: EngineJob)
    suspend fun finished(instanceIds: List<String>): Map<String, Boolean>
    suspend fun cancel(instanceId: String)
    suspend fun correlate(message: String, runId: String, itemKey: String?): Boolean
    suspend fun activeInstances(processIds: List<String>): Long
}
