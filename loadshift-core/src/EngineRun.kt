package loadshift.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.currentCoroutineContext
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
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds
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
    private val parentCache = ParentCache()
    private val parentWaits = ConcurrentHashMap<String, Int>()
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

    fun variables(jobType: String): List<String>? = handlers[jobType]?.variables

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

    suspend fun execute(job: EngineJob, lookup: VariableLookup = VariableLookup.None): JobOutcome {
        val handler = handlers[job.type]
            ?: return JobOutcome.Abort(IllegalStateException("no handler for job type '${job.type}'"))
        return withContext(Lookup(lookup)) {
            try {
                handler.run(job, job.variables).also { parentWaits.remove(job.id) }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                failure(handler, job, e)
            }
        }
    }

    private suspend fun failure(handler: JobHandler, job: EngineJob, error: Throwable): JobOutcome {
        val policy = handler.retry
        if (error is NotYetReadable) {
            val waits = parentWaits.merge(job.id, 1, Int::plus) ?: 1
            if (waits < PARENT_ATTEMPTS) {
                val retries = (job.retries ?: policy.maxAttempts).coerceAtLeast(1)
                val backoff = (PARENT_BACKOFF * waits).coerceAtMost(PARENT_MAX_BACKOFF)
                return JobOutcome.Retry(retries, backoff, error.message.orEmpty(), error.stackTraceToString())
            }
            parentWaits.remove(job.id)
        }
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
                store(compensation.handler, letter, inlined(compensation.snapshot))
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

    private class Parents(val keys: List<String?>, val items: Lazy<List<WorkItem>>)

    private class Lookup(val lookup: VariableLookup) : AbstractCoroutineContextElement(Key) {
        companion object Key : CoroutineContext.Key<Lookup>
    }

    private class NotYetReadable(message: String) : IllegalStateException(message)

    private class ParentCache {
        private val entries = object : LinkedHashMap<String, CompletableDeferred<JsonObject?>>(16, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, CompletableDeferred<JsonObject?>>?) =
                size > PARENT_CACHE_SIZE
        }

        fun put(key: String, value: JsonObject) {
            synchronized(entries) { entries[key] = CompletableDeferred(value) }
        }

        suspend fun get(key: String, load: suspend () -> JsonObject?): JsonObject? {
            val (entry, owner) = synchronized(entries) {
                entries[key]?.let { it to false } ?: CompletableDeferred<JsonObject?>().also { entries[key] = it }.let { it to true }
            }
            if (!owner) return entry.await()
            val value = try {
                load()
            } catch (e: Throwable) {
                forget(key, entry)
                entry.completeExceptionally(e)
                throw e
            }
            if (value == null) forget(key, entry)
            entry.complete(value)
            return value
        }

        private fun forget(key: String, entry: CompletableDeferred<JsonObject?>) {
            synchronized(entries) { if (entries[key] === entry) entries.remove(key) }
        }
    }

    private fun parentRef(instance: String, variable: String, jobId: String): JsonObject = buildJsonObject {
        putJsonObject(EngineNames.PARENT_REF) {
            put(REF_INSTANCE, instance)
            put(REF_VARIABLE, variable)
            put(REF_JOB, jobId)
        }
    }

    private fun refKey(ref: JsonObject): String =
        listOf(REF_INSTANCE, REF_VARIABLE, REF_JOB).joinToString("\u0000") { (ref[it] as? JsonPrimitive)?.contentOrNull.orEmpty() }

    private suspend fun parentItem(element: JsonElement): JsonObject {
        val item = element as? JsonObject ?: error("lineage entry is not an object")
        val ref = item[EngineNames.PARENT_REF] as? JsonObject ?: return item
        val instance = (ref[REF_INSTANCE] as? JsonPrimitive)?.contentOrNull.orEmpty()
        val variable = (ref[REF_VARIABLE] as? JsonPrimitive)?.contentOrNull.orEmpty()
        val lookup = currentCoroutineContext()[Lookup]?.lookup ?: VariableLookup.None
        return parentCache.get(refKey(ref)) { asObject(lookup.variable(instance, variable)) }
            ?: throw NotYetReadable("parent item '$variable' of instance $instance is not readable yet")
    }

    private fun asObject(value: JsonElement?): JsonObject? = when (value) {
        is JsonObject -> value
        is JsonPrimitive -> value.contentOrNull?.let { runCatching { Json.parseToJsonElement(it) }.getOrNull() } as? JsonObject
        else -> null
    }

    private suspend fun inlined(snapshot: JsonObject): JsonObject {
        val raw = (snapshot[EngineNames.PARENTS] as? JsonPrimitive)?.contentOrNull ?: return snapshot
        val lineage = runCatching { Json.parseToJsonElement(raw).jsonArray }.getOrNull() ?: return snapshot
        val resolved = lineage.map { element ->
            try {
                parentItem(element)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                element
            }
        }
        return JsonObject(snapshot + (EngineNames.PARENTS to JsonPrimitive(JsonArray(resolved).toString())))
    }

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
                fanHandlers(owner, fan.id, fan.expand, fan.childCodec, fan.concurrency, false, codec, itemVariable, parents).forEach(::add)
                register(owner, fan.body, EngineNames.item(fan.id), listOf(codec) + parents, fan.concurrency?.let { Semaphore(it) })
            }
            is FanIn<*, *, *> -> {
                val fan = step as FanIn<WorkItem, WorkItem, Any?>
                fanHandlers(owner, fan.id, fan.expand, fan.childCodec, fan.concurrency, true, codec, itemVariable, parents).forEach(::add)
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

    private fun fanHandlers(
        owner: Workflow<*>,
        stepId: String,
        expand: suspend (WorkItem) -> Flow<WorkItem>,
        childCodec: WorkItemCodec<WorkItem>,
        concurrency: Int?,
        keepAll: Boolean,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parents: List<WorkItemCodec<WorkItem>>,
    ): List<JobHandler> {
        val wave = concurrency ?: config.waveSize
        return listOf(
            ExpandHandler(owner, EngineNames.fanOut(stepId), stepId, expand, childCodec, codec, itemVariable, parents, wave, keepAll),
            WaveHandler(owner, stepId, codec, itemVariable),
            ExpandHandler(owner, EngineNames.expand(stepId), stepId, expand, childCodec, codec, itemVariable, parents, null, false),
        )
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

        open val reads: List<String> get() = emptyList()

        val variables: List<String>? by lazy {
            val item = itemVariable?.let(::listOf)
                ?: codec.fields?.plus(listOf(EngineNames.CALL_ITEM, EngineNames.PARENTS))
                ?: return@lazy null
            val compensations = compensable.filter { it.owner.key == owner.key }.map { EngineNames.compensation(it.topic) }
            (item + EngineNames.ITEM_KEY + EngineNames.WORKFLOW + compensations + reads).distinct()
        }

        suspend fun snapshot(variables: JsonObject): JsonObject? {
            val raw = runCatching {
                val source = source(variables)
                JsonObject(lineage(source) + codec.encode(codec.decode(source)))
            }.getOrNull() ?: return null
            return inlined(raw)
        }

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

        suspend fun parents(source: JsonObject): Parents {
            val array = ancestors(source)
            val resolved = parentCodecs.mapIndexedNotNull { index, parentCodec ->
                array.getOrNull(index)?.let { parentItem(it) }?.let { it to parentCodec }
            }
            val keys = resolved.map { (json, parentCodec) ->
                val stored = json[EngineNames.ITEM_KEY] as? JsonPrimitive
                if (stored == null) parentCodec.decode(json).key else stored.contentOrNull?.ifEmpty { null }
            }
            return Parents(keys, lazy { resolved.map { (json, parentCodec) -> parentCodec.decode(json) } })
        }

        fun lineage(source: JsonObject): Map<String, JsonElement> =
            source.filterKeys { it == EngineNames.ITEM_KEY || it == EngineNames.PARENTS }

        fun context(item: WorkItem, parents: Parents, topic: String?): CoroutineContext {
            val path = parents.keys.asReversed().map { it ?: "?" }
            val execution = ExecutionContext(runId, owner.name, config.logSink, path, item.key, topic)
            return if (parents.keys.isEmpty()) execution else execution + ParentItemStack(parents.items)
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

        private suspend fun invoke(item: WorkItem, parents: Parents) {
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
        override val reads get() = listOf(EngineNames.iterations(step.id))

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
        topic: String,
        private val stepId: String,
        private val expand: suspend (WorkItem) -> Flow<WorkItem>,
        private val childCodec: WorkItemCodec<WorkItem>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
        private val wave: Int?,
        private val keepAll: Boolean,
    ) : JobHandler(owner, topic, codec, itemVariable, parentCodecs, RetryPolicy.None) {
        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val source = source(variables)
            val item = codec.decode(source)
            val children = withContext(context(item, parents(source), null)) { expand(item).toList() }
            counters.expanded(children.size.toLong())
            val parentVariable = EngineNames.parent(stepId)
            val parent = JsonObject(codec.encode(item) + (EngineNames.ITEM_KEY to JsonPrimitive(item.key.orEmpty())))
            val ref = parentRef(job.instanceId, parentVariable, job.id)
            parentCache.put(refKey(ref.getValue(EngineNames.PARENT_REF).jsonObject), parent)
            val lineage = JsonPrimitive(JsonArray(listOf(ref) + ancestors(source)).toString())
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
            val waves = buildMap {
                if (wave == null) {
                    put(EngineNames.items(stepId), items)
                } else {
                    val pages = items.chunked(wave)
                    put(EngineNames.items(stepId), JsonArray(pages.firstOrNull().orEmpty()))
                    for (index in 1 until pages.size) put(EngineNames.page(stepId, index), JsonArray(pages[index]))
                    put(EngineNames.waves(stepId), JsonPrimitive(pages.size))
                    put(EngineNames.cursor(stepId), JsonPrimitive(1))
                    if (keepAll) put(EngineNames.all(stepId), items)
                }
            }
            val written = write(variables, source, item) + waves + (parentVariable to parent)
            return JobOutcome.Complete(JsonObject(written))
        }
    }

    private inner class WaveHandler(
        owner: Workflow<*>,
        private val stepId: String,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
    ) : JobHandler(owner, EngineNames.wave(stepId), codec, itemVariable, emptyList(), RetryPolicy.None) {
        override val reads get() = listOf(EngineNames.cursor(stepId), EngineNames.waves(stepId))

        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val count = (variables[EngineNames.waves(stepId)] as? JsonPrimitive)?.intOrNull ?: 0
            val next = (variables[EngineNames.cursor(stepId)] as? JsonPrimitive)?.intOrNull ?: count
            if (next >= count) {
                return JobOutcome.Complete(
                    JsonObject(mapOf(EngineNames.items(stepId) to JsonArray(emptyList()), EngineNames.more(stepId) to JsonPrimitive(false))),
                )
            }
            val name = EngineNames.page(stepId, next)
            val lookup = currentCoroutineContext()[Lookup]?.lookup ?: VariableLookup.None
            val page = lookup.variable(job.instanceId, name)?.let(::array)?.takeIf { it.isNotEmpty() }
                ?: throw NotYetReadable("wave '$name' of instance ${job.instanceId} is not readable yet")
            return JobOutcome.Complete(
                JsonObject(
                    mapOf(
                        EngineNames.items(stepId) to page,
                        name to JsonNull,
                        EngineNames.cursor(stepId) to JsonPrimitive(next + 1),
                        EngineNames.more(stepId) to JsonPrimitive(true),
                    ),
                ),
            )
        }
    }

    private fun array(raw: JsonElement?): JsonArray = when (raw) {
        is JsonArray -> raw
        is JsonPrimitive -> raw.contentOrNull?.let { runCatching { Json.parseToJsonElement(it) }.getOrNull() } as? JsonArray
        else -> null
    } ?: JsonArray(emptyList())

    private inner class ReduceHandler(
        owner: Workflow<*>,
        private val step: FanIn<WorkItem, WorkItem, Any?>,
        codec: WorkItemCodec<WorkItem>,
        itemVariable: String?,
        parentCodecs: List<WorkItemCodec<WorkItem>>,
    ) : JobHandler(owner, EngineNames.reduce(step.id), codec, itemVariable, parentCodecs, RetryPolicy.None) {
        override val reads get() = listOf(EngineNames.items(step.id), EngineNames.all(step.id))

        override suspend fun run(job: EngineJob, variables: JsonObject): JobOutcome {
            val source = source(variables)
            val item = codec.decode(source)
            val children = array(variables[EngineNames.all(step.id)] ?: variables[EngineNames.items(step.id)])
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
        override val reads get() = listOf(EngineNames.formVariable(step.id))

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
        override val reads get() = listOf(EngineNames.messageVariable(step.message))

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
        override val reads get() = listOf(EngineNames.callItem(stepId))

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
        const val REF_INSTANCE = "instance"
        const val REF_VARIABLE = "variable"
        const val REF_JOB = "job"
        const val PARENT_CACHE_SIZE = 64
        const val PARENT_ATTEMPTS = 30
        val PARENT_BACKOFF = 1.seconds
        val PARENT_MAX_BACKOFF = 15.seconds
    }
}
