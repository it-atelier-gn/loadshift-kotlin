package loadshift.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import java.util.Collections
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import kotlin.time.Clock
import kotlin.time.TimeSource

@EngineApi
class EngineRun(
    val workflow: Workflow<*>,
    val config: RunConfig,
    val runId: String = UUID.randomUUID().toString(),
) {
    private val counters = RunMetrics(config.metrics, workflow.key)
    private val deadLetters = Collections.synchronizedList(mutableListOf<DeadLetter>())
    private val terminated = ConcurrentHashMap<String, ItemState>()
    private val globalLimiter = config.rateLimit?.let { RateLimiter(it) }
    private val levelKeys = mutableMapOf<Pair<String, String?>, String>()
    private val compensable = mutableListOf<TaskHandler>()
    private val called = workflow.calledWorkflows()
    private val handlers: Map<String, JobHandler> = buildMap {
        for (owner in listOf(workflow) + called) register(owner, owner.root, null, emptyList(), null)
    }
    private val compensators: Map<Pair<String, String>, TaskHandler> =
        compensable.associateBy { it.owner.key to EngineNames.compensation(it.topic) }

    init {
        for ((variable, tasks) in compensable.groupBy { it.owner.key to EngineNames.compensation(it.topic) }) {
            require(tasks.size == 1) {
                "topics ${tasks.joinToString { "'${it.topic}'" }} of workflow '${variable.first}' share the compensation " +
                    "variable '${variable.second}'; rename one of them"
            }
        }
    }

    val jobTypes: List<String> = handlers.keys.toList()

    val correlationKeys: List<String> = listOf(workflow.key) + called.map { it.key }

    fun maxAttempts(jobType: String): Int =
        requireNotNull(handlers[jobType]) { "unknown job type '$jobType'" }.retry.maxAttempts

    fun progress(): Progress = counters.progress()

    fun deadLetters(): List<DeadLetter> = synchronized(deadLetters) { deadLetters.toList() }

    fun result(): RunResult = counters.result(deadLetters())

    fun recordCancelled() {
        counters.cancelled()
    }

    fun recordSkipped() {
        counters.skipped()
    }

    suspend fun admit(item: WorkItem, seen: MutableSet<String>?): Boolean {
        val key = item.key
        if (key != null && seen != null && !seen.add(key)) {
            counters.skipped()
            return false
        }
        if (key != null && config.resume && config.checkpoints?.isComplete(workflow.key, key) == true) {
            counters.skipped()
            return false
        }
        counters.seeded()
        return true
    }

    fun recordLockExtension(job: EngineJob, success: Boolean) {
        counters.lockExtension(handlers[job.type]?.topic ?: job.type, success)
    }

    fun recordAttached(count: Int) {
        counters.seeded(count.toLong())
    }

    @Suppress("UNCHECKED_CAST")
    fun rootVariables(item: WorkItem): JsonObject {
        val codec = workflow.root.codec as WorkItemCodec<WorkItem>
        return JsonObject(
            codec.encode(item) + mapOf(
                EngineNames.ITEM_KEY to JsonPrimitive(item.key.orEmpty()),
                EngineNames.WORKFLOW to JsonPrimitive(workflow.key),
            ),
        )
    }

    suspend fun rootFinished(instanceId: String, itemKey: String?, completed: Boolean): ItemState {
        terminated.remove(instanceId)?.let { return it }
        if (!completed) return ItemState.Cancelled
        counters.done()
        if (itemKey != null) config.checkpoints?.markComplete(workflow.key, itemKey)
        return ItemState.Done
    }

    fun clearInstanceState() {
        terminated.clear()
    }

    suspend fun execute(job: EngineJob): JobOutcome {
        val handler = handlers[job.type]
            ?: return JobOutcome.Abort(IllegalStateException("no handler for job type '${job.type}'"))
        return try {
            handler.run(job, job.variables)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            failure(handler, job, e)
        }
    }

    private suspend fun failure(handler: JobHandler, job: EngineJob, error: Throwable): JobOutcome {
        val policy = handler.retry
        val remaining = (job.retries ?: policy.maxAttempts).coerceAtLeast(1)
        val message = error.message ?: error.toString()
        if (remaining > 1 && policy.retryOn(error)) {
            val attempt = (policy.maxAttempts - remaining + 1).coerceAtLeast(1)
            counters.retry(handler.topic)
            return JobOutcome.Retry(remaining - 1, policy.backoff(attempt), message, error.stackTraceToString())
        }
        val key = runCatching { handler.key(handler.source(job.variables)) }.getOrNull()
        return when (config.onError) {
            ErrorPolicy.DeadLetter -> deadLetter(handler, job, DeadLetter(key, handler.topic, message))
            ErrorPolicy.Skip -> {
                counters.skipped()
                terminated[job.instanceId] = ItemState.Skipped
                JobOutcome.Terminate(message, outcome(ErrorPolicy.Skip, handler.topic, message))
            }
            ErrorPolicy.Fail -> {
                counters.failed()
                JobOutcome.Abort(error)
            }
        }
    }

    private suspend fun deadLetter(handler: JobHandler, job: EngineJob, letter: DeadLetter): JobOutcome.Terminate {
        record(handler, job, letter)
        return JobOutcome.Terminate(letter.error, outcome(ErrorPolicy.DeadLetter, letter.topic, letter.error))
    }

    private suspend fun record(handler: JobHandler, job: EngineJob, letter: DeadLetter) {
        deadLetters += letter
        counters.deadLetter(letter.topic)
        terminated[job.instanceId] = ItemState.DeadLettered
        store(handler, letter, handler.snapshot(job.variables))
        compensate(job.variables)
    }

    private suspend fun store(handler: JobHandler, letter: DeadLetter, snapshot: JsonObject?) {
        val store = config.deadLetters ?: return
        store.record(
            DeadLetterRecord(
                id = UUID.randomUUID().toString(),
                workflowKey = handler.owner.key,
                level = levelKeys.getValue(handler.owner.key to handler.itemVariable),
                itemVariable = handler.itemVariable,
                deadLetter = letter,
                item = snapshot ?: JsonObject(emptyMap()),
                recordedAt = Clock.System.now(),
                runId = runId,
            ),
        )
    }

    fun requeueVariables(record: DeadLetterRecord): JsonObject {
        val workflowEntry = EngineNames.WORKFLOW to JsonPrimitive(workflow.key)
        val itemVariable = record.itemVariable ?: return JsonObject(record.item + workflowEntry)
        val key = record.item[EngineNames.ITEM_KEY] ?: JsonPrimitive("")
        return JsonObject(mapOf(itemVariable to record.item, EngineNames.ITEM_KEY to key, workflowEntry))
    }

    private suspend fun compensate(variables: JsonObject) {
        val owner = (variables[EngineNames.WORKFLOW] as? JsonPrimitive)?.contentOrNull ?: workflow.key
        val pending = variables.flatMap { (name, value) ->
            val handler = compensators[owner to name] ?: return@flatMap emptyList()
            snapshots(value).mapIndexedNotNull { index, element ->
                val entry = element as? JsonObject ?: return@mapIndexedNotNull null
                val snapshot = entry[SNAPSHOT_ITEM] as? JsonObject ?: return@mapIndexedNotNull null
                val at = (entry[SNAPSHOT_AT] as? JsonPrimitive)?.longOrNull ?: 0L
                PendingCompensation(handler, at, index, snapshot)
            }
        }.sortedWith(
            compareByDescending<PendingCompensation> { it.at }.thenByDescending { it.handler.order }.thenByDescending { it.index },
        )
        for (compensation in pending) {
            try {
                compensation.handler.compensate(compensation.snapshot)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                val letter = DeadLetter(
                    compensation.handler.key(compensation.snapshot),
                    EngineNames.compensate(compensation.handler.topic),
                    e.message ?: e.toString(),
                )
                deadLetters += letter
                counters.deadLetter(letter.topic)
                store(compensation.handler, letter, compensation.snapshot)
            }
        }
    }

    private fun outcome(policy: ErrorPolicy, topic: String, error: String): JsonObject = buildJsonObject {
        putJsonObject(EngineNames.OUTCOME) {
            put("policy", policy.name)
            put("topic", topic)
            put("error", error)
        }
    }

    private class PendingCompensation(val handler: TaskHandler, val at: Long, val index: Int, val snapshot: JsonObject)

    private fun snapshots(value: JsonElement?): List<JsonElement> = when (value) {
        is JsonArray -> value
        is JsonObject -> listOf(value)
        is JsonPrimitive -> value.contentOrNull
            ?.let { runCatching { Json.parseToJsonElement(it) }.getOrNull() }
            ?.takeIf { it !is JsonPrimitive }
            ?.let(::snapshots)
            .orEmpty()
        else -> emptyList()
    }

    private fun MutableMap<String, JobHandler>.register(
        owner: Workflow<*>,
        level: SubFlow<*>,
        itemVariable: String?,
        parents: List<WorkItemCodec<WorkItem>>,
        limit: Semaphore?,
    ) {
        levelKeys[owner.key to itemVariable] = level.key
        @Suppress("UNCHECKED_CAST")
        collect(owner, level.step, level.codec as WorkItemCodec<WorkItem>, itemVariable, parents, limit)
    }

    @Suppress("UNCHECKED_CAST")
    private fun MutableMap<String, JobHandler>.collect(
        owner: Workflow<*>,
        step: Step<*>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parents: List<WorkItemCodec<WorkItem>>,
        limit: Semaphore?,
    ) {
        fun add(handler: JobHandler) {
            val type = EngineNames.jobType(owner.key, handler.topic)
            check(put(type, handler) == null) { "job type '$type' is registered twice" }
        }

        fun descend(child: Step<*>) = collect(owner, child, codec, itemVariable, parents, limit)

        when (step) {
            is Sequence<*> -> step.steps.forEach(::descend)
            is Execute<*> -> {
                add(TaskHandler(owner, step as Execute<WorkItem>, codec, itemVariable, parents, limit))
                step.catches.forEach { descend(it.body) }
            }
            is Conditional<*> -> {
                add(ConditionHandler(owner, step as Conditional<WorkItem>, codec, itemVariable))
                descend(step.onTrue)
                step.onFalse?.let(::descend)
            }
            is Loop<*> -> {
                add(LoopHandler(owner, step as Loop<WorkItem>, codec, itemVariable))
                descend(step.body)
            }
            is Parallel<*> -> step.branches.forEach(::descend)
            is Timeout<*> -> {
                add(TimeoutHandler(owner, step as Timeout<WorkItem>, codec, itemVariable))
                descend(step.body)
            }
            is FanOut<*, *> -> {
                val fan = step as FanOut<WorkItem, WorkItem>
                add(ExpandHandler(owner, fan.id, fan.expand, fan.childCodec, codec, itemVariable, parents))
                register(owner, fan.body, EngineNames.item(fan.id), listOf(codec) + parents, fan.concurrency?.let { Semaphore(it) })
            }
            is FanIn<*, *, *> -> {
                val fan = step as FanIn<WorkItem, WorkItem, Any?>
                add(ExpandHandler(owner, fan.id, fan.expand, fan.childCodec, codec, itemVariable, parents))
                add(ReduceHandler(owner, fan, codec, itemVariable, parents))
                register(owner, fan.body, EngineNames.item(fan.id), listOf(codec) + parents, fan.concurrency?.let { Semaphore(it) })
            }
            is HumanTask<*> -> add(FormHandler(owner, step as HumanTask<WorkItem>, codec, itemVariable, parents))
            is Call<*> -> {
                add(CallHandler(owner, step.id, codec, itemVariable, parents))
                add(ReturnHandler(owner, step.id, codec, itemVariable, parents))
            }
            is AwaitMessage<*> -> {
                val await = step as AwaitMessage<WorkItem>
                if (await.onMessage != null) add(MessageHandler(owner, await, codec, itemVariable, parents))
                await.onTimeout?.let(::descend)
            }
            is Wait<*>, is AwaitSignal<*> -> Unit
        }
    }

    private abstract inner class JobHandler(
        val owner: Workflow<*>,
        val topic: String,
        val codec: WorkItemCodec<WorkItem>,
        val itemVariable: String?,
        private val parentCodecs: List<WorkItemCodec<WorkItem>>,
        val retry: RetryPolicy,
    ) {
        abstract suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome

        fun snapshot(variables: JsonObject): JsonObject? = runCatching {
            val source = source(variables)
            JsonObject(lineage(source) + codec.encode(codec.decode(source)))
        }.getOrNull()

        fun source(variables: JsonObject): JsonObject {
            val name = itemVariable ?: return variables[EngineNames.CALL_ITEM] as? JsonObject ?: variables
            return variables[name] as? JsonObject ?: error("variable '$name' is missing or not an object")
        }

        fun key(source: JsonObject): String? =
            (source[EngineNames.ITEM_KEY] as? JsonPrimitive)?.contentOrNull?.takeIf { it.isNotEmpty() }

        fun ancestors(source: JsonObject): JsonArray =
            (source[EngineNames.PARENTS] as? JsonPrimitive)?.contentOrNull
                ?.let { Json.parseToJsonElement(it).jsonArray }
                ?: JsonArray(emptyList())

        fun parents(source: JsonObject): List<WorkItem> {
            val array = ancestors(source)
            return parentCodecs.mapIndexedNotNull { index, parentCodec ->
                array.getOrNull(index)?.jsonObject?.let(parentCodec.decode)
            }
        }

        fun lineage(source: JsonObject): Map<String, JsonElement> =
            source.filterKeys { it == EngineNames.ITEM_KEY || it == EngineNames.PARENTS }

        fun context(item: WorkItem, parents: List<WorkItem>, topic: String?): CoroutineContext {
            val path = parents.asReversed().map { it.key ?: "?" }
            val execution = ExecutionContext(runId, owner.name, config.logSink, path, item.key, topic)
            return if (parents.isEmpty()) execution else execution + ParentItemStack(parents)
        }

        fun write(variables: JsonObject, source: JsonObject, item: WorkItem): JsonObject {
            val encoded = codec.encode(item)
            val name = itemVariable ?: EngineNames.CALL_ITEM.takeIf { variables[it] is JsonObject } ?: return encoded
            return JsonObject(mapOf(name to JsonObject(source + encoded)))
        }
    }

    private inner class TaskHandler(
        owner: Workflow<*>,
        private val step: Execute<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
        private val levelLimit: Semaphore?,
    ) : JobHandler(owner, step.task.topic, codec, itemVariable, parentCodecs, step.options.retry ?: config.retry) {
        val order = compensable.size
        private val limiter = step.options.rateLimit?.let { RateLimiter(it) }
        private val timeout = step.options.timeout ?: retry.timeout

        init {
            if (step.compensation != null) compensable += this
        }

        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val source = source(variables)
            val item = codec.decode(source)
            val parents = parents(source)
            try {
                if (levelLimit == null) invoke(item, parents) else levelLimit.withPermit { invoke(item, parents) }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                val caught = step.catches.firstOrNull { it.type.isInstance(e) } ?: throw e
                return JobOutcome.Catch(EngineNames.catchError(caught.id), e.message ?: e.toString(), write(variables, source, item))
            }
            val written = write(variables, source, item)
            if (step.compensation == null) return JobOutcome.Complete(written)
            val name = EngineNames.compensation(topic)
            val entry = buildJsonObject {
                put(SNAPSHOT_AT, Clock.System.now().toEpochMilliseconds())
                put(SNAPSHOT_ITEM, JsonObject(lineage(source) + codec.encode(item)))
            }
            return JobOutcome.Complete(JsonObject(written + (name to JsonArray(snapshots(variables[name]) + entry))))
        }

        suspend fun compensate(snapshot: JsonObject) {
            val action = step.compensation ?: return
            val item = codec.decode(snapshot)
            withContext(context(item, parents(snapshot), null)) { action(item) }
        }

        private suspend fun invoke(item: WorkItem, parents: List<WorkItem>) {
            globalLimiter?.acquire()
            limiter?.acquire()
            withContext(context(item, parents, topic)) {
                config.tracer.span("task $topic", mapOf("item" to item.key.orEmpty())) {
                    val started = TimeSource.Monotonic.markNow()
                    try {
                        withTaskTimeout(timeout) { step.task.execute(item) }
                        counters.taskDuration(topic, true, started.elapsedNow())
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Throwable) {
                        counters.taskDuration(topic, false, started.elapsedNow())
                        throw e
                    }
                }
            }
        }
    }

    private inner class ConditionHandler(
        owner: Workflow<*>,
        private val step: Conditional<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
    ) : JobHandler(owner, EngineNames.decision(step.id), codec, itemVariable, emptyList(), RetryPolicy.None) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val outcome = step.predicate(codec.decode(source(variables)))
            return JobOutcome.Complete(buildJsonObject { put(EngineNames.result(step.id), outcome) })
        }
    }

    private inner class LoopHandler(
        owner: Workflow<*>,
        private val step: Loop<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
    ) : JobHandler(owner, EngineNames.decision(step.id), codec, itemVariable, emptyList(), RetryPolicy.None) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val source = source(variables)
            val iterations = (variables[EngineNames.iterations(step.id)] as? JsonPrimitive)?.intOrNull ?: 0
            if (!step.predicate(codec.decode(source))) return complete(repeat = false, iterations = 0)
            if (iterations >= config.maxLoopIterations) {
                val letter = DeadLetter(
                    key(source),
                    EngineNames.loop(step.id),
                    "exceeded maxLoopIterations=${config.maxLoopIterations}",
                )
                return deadLetter(this, job, letter)
            }
            return complete(repeat = true, iterations = iterations + 1)
        }

        private fun complete(repeat: Boolean, iterations: Int) = JobOutcome.Complete(
            buildJsonObject {
                put(EngineNames.result(step.id), repeat)
                put(EngineNames.iterations(step.id), iterations)
            },
        )
    }

    private inner class TimeoutHandler(
        owner: Workflow<*>,
        private val step: Timeout<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
    ) : JobHandler(owner, EngineNames.timeout(step.id), codec, itemVariable, emptyList(), RetryPolicy.None) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val letter = DeadLetter(key(source(variables)), topic, "exceeded ${step.duration}")
            record(this, job, letter)
            return JobOutcome.Complete(outcome(ErrorPolicy.DeadLetter, topic, letter.error))
        }
    }

    private inner class ExpandHandler(
        owner: Workflow<*>,
        private val stepId: String,
        private val expand: suspend (WorkItem) -> Flow<WorkItem>,
        private val childCodec: WorkItemCodec<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
    ) : JobHandler(owner, EngineNames.expand(stepId), codec, itemVariable, parentCodecs, RetryPolicy.None) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val source = source(variables)
            val item = codec.decode(source)
            val children = withContext(context(item, parents(source), null)) { expand(item).toList() }
            counters.expanded(children.size.toLong())
            val lineage = JsonPrimitive(JsonArray(listOf(codec.encode(item)) + ancestors(source)).toString())
            val items = JsonArray(
                children.map { child ->
                    JsonObject(
                        childCodec.encode(child) + mapOf(
                            EngineNames.PARENTS to lineage,
                            EngineNames.ITEM_KEY to JsonPrimitive(child.key.orEmpty()),
                        ),
                    )
                },
            )
            return JobOutcome.Complete(JsonObject(write(variables, source, item) + (EngineNames.items(stepId) to items)))
        }
    }

    private inner class ReduceHandler(
        owner: Workflow<*>,
        private val step: FanIn<WorkItem, WorkItem, Any?>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
    ) : JobHandler(owner, EngineNames.reduce(step.id), codec, itemVariable, parentCodecs, RetryPolicy.None) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val source = source(variables)
            val item = codec.decode(source)
            val children = when (val raw = variables[EngineNames.items(step.id)]) {
                is JsonArray -> raw
                is JsonPrimitive -> Json.parseToJsonElement(raw.content).jsonArray
                else -> JsonArray(emptyList())
            }
            var accumulator = step.initial
            for (child in children) accumulator = step.combine(accumulator, step.childCodec.decode(child.jsonObject))
            withContext(context(item, parents(source), null)) { step.onComplete(item, accumulator) }
            return JobOutcome.Complete(write(variables, source, item))
        }
    }

    private inner class FormHandler(
        owner: Workflow<*>,
        private val step: HumanTask<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
    ) : JobHandler(owner, EngineNames.form(step.id), codec, itemVariable, parentCodecs, config.retry) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val name = EngineNames.formVariable(step.id)
            val form = when (val raw = variables[name]) {
                is JsonObject -> raw
                is JsonPrimitive -> raw.contentOrNull?.let { Json.parseToJsonElement(it) as? JsonObject }
                else -> null
            } ?: JsonObject(emptyMap())
            val source = source(variables)
            val item = codec.decode(source)
            withContext(context(item, parents(source), topic)) { step.onComplete(item, form) }
            return JobOutcome.Complete(JsonObject(write(variables, source, item) + (name to JsonNull)))
        }
    }

    private inner class MessageHandler(
        owner: Workflow<*>,
        private val step: AwaitMessage<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
    ) : JobHandler(owner, EngineNames.message(step.id), codec, itemVariable, parentCodecs, config.retry) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val name = EngineNames.messageVariable(step.message)
            val data = when (val raw = variables[name]) {
                is JsonObject -> raw
                is JsonPrimitive -> raw.contentOrNull?.let { Json.parseToJsonElement(it) as? JsonObject }
                else -> null
            } ?: JsonObject(emptyMap())
            val source = source(variables)
            val item = codec.decode(source)
            val onMessage = requireNotNull(step.onMessage)
            withContext(context(item, parents(source), topic)) { onMessage(item, data) }
            return JobOutcome.Complete(JsonObject(write(variables, source, item) + (name to JsonNull)))
        }
    }

    private inner class CallHandler(
        owner: Workflow<*>,
        private val stepId: String,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
    ) : JobHandler(owner, EngineNames.call(stepId), codec, itemVariable, parentCodecs, RetryPolicy.None) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val item = codec.decode(source(variables))
            val packed = JsonObject(codec.encode(item) + (EngineNames.ITEM_KEY to JsonPrimitive(item.key.orEmpty())))
            return JobOutcome.Complete(JsonObject(mapOf(EngineNames.callItem(stepId) to packed)))
        }
    }

    private inner class ReturnHandler(
        owner: Workflow<*>,
        private val stepId: String,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
    ) : JobHandler(owner, EngineNames.returnCall(stepId), codec, itemVariable, parentCodecs, RetryPolicy.None) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val name = EngineNames.callItem(stepId)
            val returned = when (val raw = variables[name]) {
                is JsonObject -> raw
                is JsonPrimitive -> raw.contentOrNull?.let { Json.parseToJsonElement(it) as? JsonObject }
                else -> null
            } ?: return JobOutcome.Complete(JsonObject(mapOf(name to JsonNull)))
            val source = source(variables)
            val item = codec.decode(returned)
            return JobOutcome.Complete(JsonObject(write(variables, source, item) + (name to JsonNull)))
        }
    }

    private companion object {
        const val SNAPSHOT_AT = "at"
        const val SNAPSHOT_ITEM = "item"
    }
}
