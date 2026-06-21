package loadshift.camunda8

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.datetime.Clock
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import loadshift.core.BpmnCompiler
import loadshift.core.Conditional
import loadshift.core.CronSchedule
import loadshift.core.awaitNext
import loadshift.core.Execute
import loadshift.core.ExecutionContext
import loadshift.core.FanOut
import loadshift.core.FanIn
import loadshift.core.Wait
import loadshift.core.Timeout
import loadshift.core.AwaitMessage
import loadshift.core.ControllableBackend
import loadshift.core.Loop
import loadshift.core.Parallel
import loadshift.core.ParentItemStack
import loadshift.core.Progress
import loadshift.core.RunConfig
import loadshift.core.RunHandle
import loadshift.core.RunInspector
import loadshift.core.RunResult
import loadshift.core.RunState
import loadshift.core.RunTracker
import loadshift.core.Sequence
import loadshift.core.Start
import loadshift.core.Step
import loadshift.core.SubFlow
import loadshift.core.Task
import loadshift.core.WorkItem
import loadshift.core.WorkItemCodec
import loadshift.core.Workflow
import org.camunda.bpm.model.bpmn.Bpmn
import java.io.ByteArrayOutputStream
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

class Camunda8Backend(
    base: String = "http://localhost:8080",
    token: String? = null,
    private val client: Camunda8Client = Camunda8Client(base, token),
) : ControllableBackend {

    override val control = RunTracker("camunda8")

    override suspend fun <W : WorkItem> run(workflow: Workflow<W>, config: RunConfig): RunHandle {
        val processes = BpmnCompiler.compile(workflow)
        val resources = processes.map { p ->
            Camunda8Dialect.decorate(p.model, p.serviceTasks)
            val out = ByteArrayOutputStream()
            Bpmn.writeModelToStream(out, p.model)
            "${p.key}.bpmn" to out.toByteArray()
        }
        client.deploy(resources)

        val registry = buildRegistry(workflow)
        val run = Camunda8Run(workflow, config, client, registry, processes.map { it.key }).also { it.begin() }
        control.track(workflow, run, run)
        return run
    }
}

internal class TaskHandler(
    val task: Task<WorkItem>,
    val codec: WorkItemCodec<WorkItem>,
    val parentCodecs: List<WorkItemCodec<WorkItem>> = emptyList(),
)
internal class DecisionHandler(val id: String, val predicate: (WorkItem) -> Boolean, val codec: WorkItemCodec<WorkItem>)
internal class ExpandHandler(
    val id: String,
    val expand: suspend (WorkItem) -> Flow<WorkItem>,
    val codec: WorkItemCodec<WorkItem>,
    val childCodec: WorkItemCodec<WorkItem>,
)
internal class ReduceHandler(
    val id: String,
    val codec: WorkItemCodec<WorkItem>,
    val childCodec: WorkItemCodec<WorkItem>,
    val initial: Any?,
    val combine: (Any?, WorkItem) -> Any?,
    val onComplete: suspend (WorkItem, Any?) -> Unit,
    val parentCodecs: List<WorkItemCodec<WorkItem>> = emptyList(),
)

internal class Registry {
    val taskHandlers = mutableMapOf<String, TaskHandler>()
    val decisionHandlers = mutableMapOf<String, DecisionHandler>()
    val expandHandlers = mutableMapOf<String, ExpandHandler>()
    val reduceHandlers = mutableMapOf<String, ReduceHandler>()
    val topics: List<String>
        get() = (taskHandlers.keys + decisionHandlers.keys + expandHandlers.keys + reduceHandlers.keys).toList()
}

@Suppress("UNCHECKED_CAST")
private fun buildRegistry(workflow: Workflow<*>): Registry {
    val registry = Registry()
    populateLevel(workflow.root, emptyList(), registry)
    return registry
}

@Suppress("UNCHECKED_CAST")
private fun populateLevel(level: SubFlow<*>, parentCodecs: List<WorkItemCodec<WorkItem>>, registry: Registry) {
    val codec = level.codec as WorkItemCodec<WorkItem>
    for ((topic, predicate) in level.decisions) {
        val id = topic.removePrefix("decision_")
        registry.decisionHandlers[topic] = DecisionHandler(id, predicate as (WorkItem) -> Boolean, codec)
    }
    collectFromStep(level.step, codec, parentCodecs, registry)
}

@Suppress("UNCHECKED_CAST")
private fun collectFromStep(
    step: Step<*>,
    codec: WorkItemCodec<WorkItem>,
    parentCodecs: List<WorkItemCodec<WorkItem>>,
    registry: Registry,
) {
    when (step) {
        is Sequence -> step.steps.forEach { collectFromStep(it, codec, parentCodecs, registry) }
        is Execute<*> -> {
            val task = step.task as Task<WorkItem>
            registry.taskHandlers[task.topic] = TaskHandler(task, codec, parentCodecs)
        }
        is Conditional -> {
            collectFromStep(step.onTrue, codec, parentCodecs, registry)
            step.onFalse?.let { collectFromStep(it, codec, parentCodecs, registry) }
        }
        is Loop -> collectFromStep(step.body, codec, parentCodecs, registry)
        is Parallel -> step.branches.forEach { collectFromStep(it, codec, parentCodecs, registry) }
        is FanOut<*, *> -> {
            val fanOut = step as FanOut<WorkItem, WorkItem>
            registry.expandHandlers["expand_${fanOut.id}"] =
                ExpandHandler(fanOut.id, fanOut.expand, codec, fanOut.childCodec as WorkItemCodec<WorkItem>)
            val childParentCodecs = listOf(codec) + parentCodecs
            populateLevel(fanOut.body, childParentCodecs, registry)
        }
        is FanIn<*, *, *> -> {
            val fanIn = step as FanIn<WorkItem, WorkItem, Any?>
            registry.expandHandlers["expand_${fanIn.id}"] =
                ExpandHandler(fanIn.id, fanIn.expand, codec, fanIn.childCodec as WorkItemCodec<WorkItem>)
            registry.reduceHandlers["reduce_${fanIn.id}"] =
                ReduceHandler(
                    fanIn.id,
                    codec,
                    fanIn.childCodec as WorkItemCodec<WorkItem>,
                    fanIn.initial,
                    fanIn.combine,
                    fanIn.onComplete,
                    parentCodecs,
                )
            val childParentCodecs = listOf(codec) + parentCodecs
            populateLevel(fanIn.body, childParentCodecs, registry)
        }
        is Wait -> {}
        is Timeout -> collectFromStep(step.body, codec, parentCodecs, registry)
        is AwaitMessage -> {}
    }
}

internal class Camunda8Run(
    private val workflow: Workflow<*>,
    private val config: RunConfig,
    private val client: Camunda8Client,
    private val registry: Registry,
    private val levelKeys: List<String>,
) : RunHandle, RunInspector {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val workerId = "loadshift-${UUID.randomUUID()}"
    private val runId = UUID.randomUUID().toString()
    private val startSignal = CompletableDeferred<Unit>()
    private val completion = CompletableDeferred<RunResult>()

    private val done = AtomicLong()
    private val failed = AtomicLong()
    private val seeded = AtomicLong()

    @Volatile private var running = true
    @Volatile private var runState = if (config.start is Start.Now) RunState.Running else RunState.Scheduled

    fun begin() {
        registry.topics.forEach { topic ->
            repeat(config.maxConcurrency) { scope.launch { workerLoop(topic) } }
        }
        scope.launch {
            try {
                when (val s = config.start) {
                    is Start.Manual -> startSignal.await()
                    is Start.At -> {
                        val waitMs = s.time.toEpochMilliseconds() - Clock.System.now().toEpochMilliseconds()
                        if (waitMs > 0) delay(waitMs)
                    }
                    else -> {}
                }
                runState = RunState.Running
                when (val s = config.start) {
                    is Start.Cron -> {
                        while (running) {
                            startRootInstances()
                            awaitDrain()
                            if (!running) break
                            CronSchedule.awaitNext(s.expr)
                        }
                    }
                    else -> {
                        startRootInstances()
                        awaitDrain()
                    }
                }
                running = false
                runState = RunState.Completed
                completion.complete(RunResult(done.get(), failed.get(), 0, emptyList()))
            } catch (e: CancellationException) {
                runState = RunState.Cancelled
                completion.complete(RunResult(done.get(), failed.get(), 0, emptyList()))
            } catch (e: Throwable) {
                runState = RunState.Failed
                completion.completeExceptionally(e)
            } finally {
                running = false
            }
        }
    }

    override suspend fun start() {
        startSignal.complete(Unit)
    }

    override fun progress(): Progress = Progress(seeded.get(), 0, done.get(), failed.get(), 0)

    override suspend fun pause() {
        running = false
        if (runState == RunState.Running) runState = RunState.Paused
    }

    override suspend fun cancel() {
        running = false
        runState = RunState.Cancelled
        scope.cancel()
        if (!completion.isCompleted) completion.complete(RunResult(done.get(), failed.get(), 0, emptyList()))
    }

    override suspend fun await(): RunResult = completion.await()

    override suspend fun send(message: String, key: String) {
        client.publishMessage(message, key)
    }

    override suspend fun broadcast(message: String) {
        throw UnsupportedOperationException("Zeebe correlates messages per key; use send(message, key)")
    }

    override fun state(): RunState = runState

    override suspend fun engineActive(): Long? =
        runCatching { levelKeys.sumOf { client.instanceCount(it) } }.getOrNull()

    @Suppress("UNCHECKED_CAST")
    private suspend fun startRootInstances() {
        val rootCodec = workflow.root.codec as WorkItemCodec<WorkItem>
        val semaphore = Semaphore(config.maxConcurrency)
        val seenKeys = if (config.dedupe) HashSet<String>() else null
        coroutineScope {
            workflow.seed().collect { item ->
                val key = (item as WorkItem).key
                if (seenKeys != null && key != null && !seenKeys.add(key)) return@collect
                seeded.incrementAndGet()
                semaphore.acquire()
                launch {
                    try {
                        val encoded = rootCodec.encode(item as WorkItem)
                        val withKey = buildJsonObject {
                            encoded.forEach { (k, v) -> put(k, v) }
                            put("loadshiftKey", JsonPrimitive((item as WorkItem).key ?: ""))
                        }
                        client.createInstance(workflow.root.key, withKey)
                    } finally {
                        semaphore.release()
                    }
                }
            }
        }
    }

    private suspend fun awaitDrain() {
        var emptyPolls = 0
        while (running) {
            delay(500)
            val total = levelKeys.sumOf { client.instanceCount(it) }
            if (total == 0L) {
                if (++emptyPolls >= 3) return
            } else {
                emptyPolls = 0
            }
        }
    }

    private suspend fun workerLoop(topic: String) {
        var idle = 200L
        while (running) {
            val jobs = try {
                client.activateJobs(
                    ActivateJobsRequest(
                        type = topic,
                        worker = workerId,
                        timeout = config.lockDuration.inWholeMilliseconds,
                        maxJobsToActivate = 10,
                    ),
                )
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                delay(1000)
                continue
            }
            if (jobs.isEmpty()) {
                delay(idle)
                idle = (idle * 2).coerceAtMost(2000)
                continue
            }
            idle = 200L
            for (job in jobs) process(topic, job)
        }
    }

    private suspend fun process(topic: String, job: ActivatedJob) {
        val result = try {
            val variables = unwrapItemVariables(C8Variables.fromJson(job.variables))
            dispatch(topic, C8Variables.toJson(variables))
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            failed.incrementAndGet()
            runCatching {
                client.failJob(
                    job.jobKey,
                    FailJobRequest(
                        retries = (config.retry.maxAttempts - 1).coerceAtLeast(0),
                        errorMessage = e.message ?: "job failed",
                        retryBackOff = config.retry.baseDelay.inWholeMilliseconds,
                    ),
                )
            }
            return
        }
        completeWithRetry(job.jobKey, result)
    }

    private suspend fun completeWithRetry(jobKey: String, result: JsonObject) {
        var attempt = 0
        while (true) {
            try {
                client.completeJob(jobKey, CompleteJobRequest(result))
                done.incrementAndGet()
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                if (++attempt >= 5) {
                    runCatching {
                        client.failJob(
                            jobKey,
                            FailJobRequest(retries = 1, errorMessage = e.message ?: "completion failed", retryBackOff = 500),
                        )
                    }
                    return
                }
                delay(150L * attempt)
            }
        }
    }

    private suspend fun dispatch(topic: String, variables: JsonObject): JsonObject {
        registry.taskHandlers[topic]?.let { handler ->
            val item = handler.codec.decode(variables)
            val parentsStr = (variables["loadshiftParents"] as? JsonPrimitive)?.content
            val parentStack = if (handler.parentCodecs.isNotEmpty() && parentsStr != null) {
                val arr = Json.parseToJsonElement(parentsStr).jsonArray
                val parentItems = handler.parentCodecs.mapIndexedNotNull { i, pc ->
                    arr.getOrNull(i)?.jsonObject?.let { pc.decode(it) }
                }
                if (parentItems.isNotEmpty()) ParentItemStack(parentItems) else null
            } else null
            val execCtx = ExecutionContext(runId, workflow.name, config.logSink, itemKey = item.key, topic = topic)
            withContext(if (parentStack != null) execCtx + parentStack else execCtx) {
                config.tracer.span("task $topic", mapOf("item" to (item.key ?: ""))) {
                    handler.task.execute(item)
                }
            }
            return handler.codec.encode(item)
        }
        registry.decisionHandlers[topic]?.let { handler ->
            val item = handler.codec.decode(variables)
            val result = handler.predicate(item)
            return buildJsonObject { put("${handler.id}_result", C8Variables.encode(result)) }
        }
        registry.expandHandlers[topic]?.let { handler ->
            val item = handler.codec.decode(variables)
            val children = handler.expand(item).toList()
            val currentParentJson = handler.codec.encode(item)
            val existingParentsStr = (variables["loadshiftParents"] as? JsonPrimitive)?.content
            val existingParents = existingParentsStr?.let { Json.parseToJsonElement(it).jsonArray }
                ?: JsonArray(emptyList())
            val newParentsJson = buildJsonArray {
                add(currentParentJson)
                existingParents.forEach { add(it) }
            }
            val parentsStr = JsonPrimitive(newParentsJson.toString())
            val array = JsonArray(children.map { child ->
                buildJsonObject {
                    handler.childCodec.encode(child).forEach { (k, v) -> put(k, v) }
                    put("loadshiftParents", parentsStr)
                }
            })
            return buildJsonObject { put("${handler.id}_items", array) }
        }
        registry.reduceHandlers[topic]?.let { handler ->
            val item = handler.codec.decode(variables)
            val children = when (val r = variables["${handler.id}_items"]) {
                is JsonArray -> r
                is JsonPrimitive -> Json.parseToJsonElement(r.content).jsonArray
                else -> JsonArray(emptyList())
            }
            var acc = handler.initial
            for (el in children) {
                acc = handler.combine(acc, handler.childCodec.decode(el.jsonObject))
            }
            val parentsStr = (variables["loadshiftParents"] as? JsonPrimitive)?.content
            val parentStack = if (handler.parentCodecs.isNotEmpty() && parentsStr != null) {
                val arr = Json.parseToJsonElement(parentsStr).jsonArray
                val parentItems = handler.parentCodecs.mapIndexedNotNull { i, pc ->
                    arr.getOrNull(i)?.jsonObject?.let { pc.decode(it) }
                }
                if (parentItems.isNotEmpty()) ParentItemStack(parentItems) else null
            } else null
            val execCtx = ExecutionContext(runId, workflow.name, config.logSink, itemKey = item.key, topic = topic)
            withContext(if (parentStack != null) execCtx + parentStack else execCtx) {
                handler.onComplete(item, acc)
            }
            return handler.codec.encode(item)
        }
        error("no handler for topic '$topic'")
    }

    private fun unwrapItemVariables(variables: MutableMap<String, Any?>): MutableMap<String, Any?> {
        for ((key, value) in variables.toList()) {
            if (key.endsWith("_item") && value is Map<*, *>) {
                for ((k, v) in value) {
                    val name = k as? String ?: continue
                    if (name !in variables) variables[name] = v
                }
            }
        }
        return variables
    }
}
