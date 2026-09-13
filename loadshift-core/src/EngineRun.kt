package loadshift.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.util.Collections
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

@EngineApi
class EngineRun(
    val workflow: Workflow<*>,
    val config: RunConfig,
    val runId: String = UUID.randomUUID().toString(),
) {
    private val seeded = AtomicLong()
    private val expanded = AtomicLong()
    private val done = AtomicLong()
    private val failed = AtomicLong()
    private val skipped = AtomicLong()
    private val deadLetters = Collections.synchronizedList(mutableListOf<DeadLetter>())
    private class Compensation(val topic: String, val key: String?, val action: suspend () -> Unit)

    private val compensations = ConcurrentHashMap<String, MutableList<Compensation>>()
    private val terminated: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val globalLimiter = config.rateLimit?.let { RateLimiter(it) }
    private val handlers: Map<String, JobHandler> = buildMap { register(workflow.root, null, emptyList(), null) }

    val jobTypes: List<String> = handlers.keys.toList()

    fun maxAttempts(jobType: String): Int =
        requireNotNull(handlers[jobType]) { "unknown job type '$jobType'" }.retry.maxAttempts

    fun progress(): Progress = Progress(seeded.get(), expanded.get(), done.get(), failed.get(), skipped.get())

    fun deadLetters(): List<DeadLetter> = synchronized(deadLetters) { deadLetters.toList() }

    fun result(): RunResult = RunResult(done.get(), failed.get(), skipped.get(), deadLetters())

    suspend fun admit(item: WorkItem, seen: MutableSet<String>?): Boolean {
        val key = item.key
        if (key != null && seen != null && !seen.add(key)) {
            skipped.incrementAndGet()
            return false
        }
        if (key != null && config.resume && config.checkpoints?.isComplete(workflow.key, key) == true) {
            skipped.incrementAndGet()
            return false
        }
        seeded.incrementAndGet()
        return true
    }

    @Suppress("UNCHECKED_CAST")
    fun rootVariables(item: WorkItem): JsonObject {
        val codec = workflow.root.codec as WorkItemCodec<WorkItem>
        return JsonObject(
            codec.encode(item) + mapOf(
                EngineNames.ITEM_KEY to JsonPrimitive(item.key.orEmpty()),
                EngineNames.RUN_ID to JsonPrimitive(runId),
            ),
        )
    }

    suspend fun rootFinished(instanceId: String, itemKey: String?, completed: Boolean) {
        compensations.remove(instanceId)
        if (terminated.remove(instanceId) || !completed) return
        done.incrementAndGet()
        if (itemKey != null) config.checkpoints?.markComplete(workflow.key, itemKey)
    }

    fun clearInstanceState() {
        compensations.clear()
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
            return JobOutcome.Retry(remaining - 1, policy.backoff(attempt), message, error.stackTraceToString())
        }
        val key = runCatching { handler.key(handler.source(job.variables)) }.getOrNull()
        return when (config.onError) {
            ErrorPolicy.DeadLetter -> terminate(job.instanceId, DeadLetter(key, handler.topic, message))
            ErrorPolicy.Skip -> {
                skipped.incrementAndGet()
                terminated += job.instanceId
                compensations.remove(job.instanceId)
                JobOutcome.Terminate(message)
            }
            ErrorPolicy.Fail -> {
                failed.incrementAndGet()
                JobOutcome.Abort(error)
            }
        }
    }

    private suspend fun terminate(instanceId: String, letter: DeadLetter): JobOutcome.Terminate {
        record(instanceId, letter)
        return JobOutcome.Terminate(letter.error)
    }

    private suspend fun record(instanceId: String, letter: DeadLetter) {
        deadLetters += letter
        terminated += instanceId
        val pending = compensations.remove(instanceId) ?: return
        for (compensation in synchronized(pending) { pending.toList() }.asReversed()) {
            try {
                compensation.action()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                deadLetters += DeadLetter(compensation.key, EngineNames.compensate(compensation.topic), e.message ?: e.toString())
            }
        }
    }

    private fun MutableMap<String, JobHandler>.register(
        level: SubFlow<*>,
        itemVariable: String?,
        parents: List<WorkItemCodec<WorkItem>>,
        limit: Semaphore?,
    ) {
        @Suppress("UNCHECKED_CAST")
        collect(level.step, level.codec as WorkItemCodec<WorkItem>, itemVariable, parents, limit)
    }

    @Suppress("UNCHECKED_CAST")
    private fun MutableMap<String, JobHandler>.collect(
        step: Step<*>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parents: List<WorkItemCodec<WorkItem>>,
        limit: Semaphore?,
    ) {
        fun add(handler: JobHandler) {
            val type = EngineNames.jobType(workflow.key, handler.topic)
            check(put(type, handler) == null) { "job type '$type' is registered twice" }
        }

        fun descend(child: Step<*>) = collect(child, codec, itemVariable, parents, limit)

        when (step) {
            is Sequence<*> -> step.steps.forEach(::descend)
            is Execute<*> -> add(TaskHandler(step as Execute<WorkItem>, codec, itemVariable, parents, limit))
            is Conditional<*> -> {
                add(ConditionHandler(step as Conditional<WorkItem>, codec, itemVariable))
                descend(step.onTrue)
                step.onFalse?.let(::descend)
            }
            is Loop<*> -> {
                add(LoopHandler(step as Loop<WorkItem>, codec, itemVariable))
                descend(step.body)
            }
            is Parallel<*> -> step.branches.forEach(::descend)
            is Timeout<*> -> {
                add(TimeoutHandler(step as Timeout<WorkItem>, codec, itemVariable))
                descend(step.body)
            }
            is FanOut<*, *> -> {
                val fan = step as FanOut<WorkItem, WorkItem>
                add(ExpandHandler(fan.id, fan.expand, fan.childCodec, codec, itemVariable, parents))
                register(fan.body, EngineNames.item(fan.id), listOf(codec) + parents, fan.concurrency?.let { Semaphore(it) })
            }
            is FanIn<*, *, *> -> {
                val fan = step as FanIn<WorkItem, WorkItem, Any?>
                add(ExpandHandler(fan.id, fan.expand, fan.childCodec, codec, itemVariable, parents))
                add(ReduceHandler(fan, codec, itemVariable, parents))
                register(fan.body, EngineNames.item(fan.id), listOf(codec) + parents, fan.concurrency?.let { Semaphore(it) })
            }
            is Wait<*>, is AwaitMessage<*> -> Unit
        }
    }

    private abstract inner class JobHandler(
        val topic: String,
        val codec: WorkItemCodec<WorkItem>,
        private val itemVariable: String?,
        private val parentCodecs: List<WorkItemCodec<WorkItem>>,
        val retry: RetryPolicy,
    ) {
        abstract suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome

        fun source(variables: JsonObject): JsonObject {
            val name = itemVariable ?: return variables
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

        fun context(item: WorkItem, parents: List<WorkItem>, topic: String?): CoroutineContext {
            val path = parents.asReversed().map { it.key ?: "?" }
            val execution = ExecutionContext(runId, workflow.name, config.logSink, path, item.key, topic)
            return if (parents.isEmpty()) execution else execution + ParentItemStack(parents)
        }

        fun write(source: JsonObject, item: WorkItem): JsonObject {
            val encoded = codec.encode(item)
            val name = itemVariable ?: return encoded
            return JsonObject(mapOf(name to JsonObject(source + encoded)))
        }
    }

    private inner class TaskHandler(
        private val step: Execute<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
        private val levelLimit: Semaphore?,
    ) : JobHandler(step.task.topic, codec, itemVariable, parentCodecs, step.options.retry ?: config.retry) {
        private val limiter = step.options.rateLimit?.let { RateLimiter(it) }
        private val timeout = step.options.timeout ?: retry.timeout

        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val source = source(variables)
            val item = codec.decode(source)
            val parents = parents(source)
            if (levelLimit == null) invoke(item, parents) else levelLimit.withPermit { invoke(item, parents) }
            step.compensation?.let { compensate ->
                compensations.computeIfAbsent(job.instanceId) { Collections.synchronizedList(mutableListOf()) }
                    .add(Compensation(topic, item.key) { compensate(item) })
            }
            return JobOutcome.Complete(write(source, item))
        }

        private suspend fun invoke(item: WorkItem, parents: List<WorkItem>) {
            globalLimiter?.acquire()
            limiter?.acquire()
            withContext(context(item, parents, topic)) {
                config.tracer.span("task $topic", mapOf("item" to item.key.orEmpty())) {
                    withTaskTimeout(timeout) { step.task.execute(item) }
                }
            }
        }
    }

    private inner class ConditionHandler(
        private val step: Conditional<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
    ) : JobHandler(EngineNames.decision(step.id), codec, itemVariable, emptyList(), RetryPolicy.None) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val outcome = step.predicate(codec.decode(source(variables)))
            return JobOutcome.Complete(buildJsonObject { put(EngineNames.result(step.id), outcome) })
        }
    }

    private inner class LoopHandler(
        private val step: Loop<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
    ) : JobHandler(EngineNames.decision(step.id), codec, itemVariable, emptyList(), RetryPolicy.None) {
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
                return terminate(job.instanceId, letter)
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
        private val step: Timeout<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
    ) : JobHandler(EngineNames.timeout(step.id), codec, itemVariable, emptyList(), RetryPolicy.None) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val letter = DeadLetter(key(source(variables)), topic, "exceeded ${step.duration}")
            record(job.instanceId, letter)
            return JobOutcome.Complete(JsonObject(emptyMap()))
        }
    }

    private inner class ExpandHandler(
        private val stepId: String,
        private val expand: suspend (WorkItem) -> Flow<WorkItem>,
        private val childCodec: WorkItemCodec<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
    ) : JobHandler(EngineNames.expand(stepId), codec, itemVariable, parentCodecs, RetryPolicy.None) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val source = source(variables)
            val item = codec.decode(source)
            val children = withContext(context(item, parents(source), null)) { expand(item).toList() }
            expanded.addAndGet(children.size.toLong())
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
            return JobOutcome.Complete(JsonObject(write(source, item) + (EngineNames.items(stepId) to items)))
        }
    }

    private inner class ReduceHandler(
        private val step: FanIn<WorkItem, WorkItem, Any?>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
    ) : JobHandler(EngineNames.reduce(step.id), codec, itemVariable, parentCodecs, RetryPolicy.None) {
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
            return JobOutcome.Complete(write(source, item))
        }
    }
}
