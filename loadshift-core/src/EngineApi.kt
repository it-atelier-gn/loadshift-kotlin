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
    const val WORKFLOW = "loadshiftWorkflow"
    const val PARENTS = "loadshiftParents"
    const val OUTCOME = "loadshiftOutcome"
    const val COMPENSATION_PREFIX = "loadshiftCompensation_"
    const val TERMINATE_ERROR = "loadshift-terminate"
    const val CALL_ITEM = "loadshiftCallItem"
    const val NEXT_TICK = "loadshiftNextTick"
    const val SCHEDULE_NEXT = "loadshift_schedule_next"
    const val SCHEDULE_SEED = "loadshift_schedule_seed"
    const val SCHEDULE_AWAIT = "loadshift_schedule_await"

    private val NON_IDENTIFIER = Regex("[^A-Za-z0-9_]")

    fun jobType(workflowKey: String, name: String): String = "$workflowKey/$name"
    fun scheduleProcess(workflowKey: String): String = "${workflowKey}_loadshift_schedule"
    fun decision(stepId: String): String = "decision_$stepId"
    fun call(stepId: String): String = "call_$stepId"
    fun returnCall(stepId: String): String = "return_$stepId"
    fun callActivity(stepId: String): String = "call_activity_$stepId"
    fun callItem(stepId: String): String = "${stepId}_call"
    fun userTask(stepId: String): String = "user_$stepId"
    fun form(stepId: String): String = "form_$stepId"
    fun formVariable(stepId: String): String = "${stepId}_form"
    fun userTaskStep(elementId: String): String? = elementId.takeIf { it.startsWith("user_") }?.removePrefix("user_")
    fun expand(stepId: String): String = "expand_$stepId"
    fun reduce(stepId: String): String = "reduce_$stepId"
    fun timeout(stepId: String): String = "timeout_$stepId"
    fun loop(stepId: String): String = "loop_$stepId"
    fun compensate(topic: String): String = "compensate_$topic"
    fun compensation(topic: String): String = COMPENSATION_PREFIX + topic.replace(NON_IDENTIFIER, "_")
    fun items(stepId: String): String = "${stepId}_items"
    fun item(stepId: String): String = "${stepId}_item"
    fun result(stepId: String): String = "${stepId}_result"
    fun iterations(stepId: String): String = "${stepId}_iterations"
    fun correlationKey(workflowKey: String, itemKey: String): String = "$workflowKey:$itemKey"
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
class RootInstance(val id: String, val itemKey: String?)

@EngineApi
sealed interface JobOutcome {
    data class Complete(val variables: JsonObject) : JobOutcome
    data class Retry(val retries: Int, val backoff: Duration, val message: String, val details: String) : JobOutcome
    data class Terminate(val message: String, val variables: JsonObject) : JobOutcome
    data class Abort(val cause: Throwable) : JobOutcome
}

@EngineApi
interface EngineDriver {
    fun pollGroups(jobTypes: List<String>): List<List<String>>
    suspend fun startInstance(processId: String, variables: JsonObject, businessKey: String?): String
    suspend fun activeRoots(processId: String): List<RootInstance>
    suspend fun fetch(jobTypes: List<String>, maxJobs: Int, lock: Duration, wait: Duration): List<EngineJob>
    suspend fun complete(job: EngineJob, variables: JsonObject)
    suspend fun fail(job: EngineJob, retries: Int, backoff: Duration, message: String, details: String)
    suspend fun terminate(job: EngineJob, message: String, variables: JsonObject)
    suspend fun extendLock(job: EngineJob, lock: Duration)
    suspend fun release(job: EngineJob)
    suspend fun finished(instanceIds: List<String>): Map<String, Boolean>
    suspend fun cancel(instanceId: String)
    suspend fun correlate(message: String, workflowKey: String, itemKey: String?): Boolean
    suspend fun activeInstances(processIds: List<String>): Long
    suspend fun close() {}
}
