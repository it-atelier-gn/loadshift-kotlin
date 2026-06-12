package loadshift.camunda7

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.datetime.Clock
import loadshift.core.BpmnCompiler
import loadshift.core.Execute
import loadshift.core.FanOut
import loadshift.core.Conditional
import loadshift.core.IntrospectableBackend
import loadshift.core.Loop
import loadshift.core.Parallel
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
import loadshift.core.WorkItemBase
import loadshift.core.Workflow
import org.camunda.bpm.model.bpmn.Bpmn
import java.io.ByteArrayOutputStream
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

private typealias Factory = (MutableMap<String, Any?>) -> WorkItemBase

class Camunda7Backend(
    base: String = "http://localhost:8080/engine-rest",
    private val client: Camunda7Client = Camunda7Client(base),
) : IntrospectableBackend {

    override val introspection = RunTracker("camunda7")

    override suspend fun <W : WorkItemBase> run(workflow: Workflow<W>, config: RunConfig): RunHandle {
        val processes = BpmnCompiler.compile(workflow)
        val resources = processes.map { p ->
            Camunda7Dialect.decorate(p.model, p.serviceTasks)
            "${p.key}.bpmn" to toXmlBytes(p.model)
        }
        client.deploy(workflow.name, resources)

        val registry = buildRegistry(workflow)
        val run = Camunda7Run(workflow, config, client, registry, processes.map { it.key }).also { it.begin() }
        introspection.track(workflow, run)
        return run
    }

    private fun toXmlBytes(model: org.camunda.bpm.model.bpmn.BpmnModelInstance): ByteArray {
        val out = ByteArrayOutputStream()
        Bpmn.writeModelToStream(out, model)
        return out.toByteArray()
    }
}

internal class TaskHandler(val task: Task<WorkItemBase>, val factory: Factory)
internal class DecisionHandler(val id: String, val predicate: (WorkItemBase) -> Boolean, val factory: Factory)
internal class ExpandHandler(
    val id: String,
    val expand: suspend (WorkItemBase) -> Flow<WorkItemBase>,
    val factory: Factory,
)

internal class Registry {
    val taskHandlers = mutableMapOf<String, TaskHandler>()
    val decisionHandlers = mutableMapOf<String, DecisionHandler>()
    val expandHandlers = mutableMapOf<String, ExpandHandler>()
    val rootKey: String get() = rootKeyValue
    var rootKeyValue: String = ""

    val topics: List<String>
        get() = (taskHandlers.keys + decisionHandlers.keys + expandHandlers.keys).toList()
}

@Suppress("UNCHECKED_CAST")
private fun buildRegistry(workflow: Workflow<*>): Registry {
    val registry = Registry()
    registry.rootKeyValue = workflow.root.key
    val levels = mutableListOf<SubFlow<*>>()
    gather(workflow.root, levels)
    for (level in levels) {
        val factory = level.factory as Factory
        for ((topic, predicate) in level.decisions) {
            val id = topic.removePrefix("decision_")
            registry.decisionHandlers[topic] = DecisionHandler(id, predicate as (WorkItemBase) -> Boolean, factory)
        }
        collectFromStep(level.step, factory, registry)
    }
    return registry
}

@Suppress("UNCHECKED_CAST")
private fun collectFromStep(step: Step<*>, factory: Factory, registry: Registry) {
    when (step) {
        is Sequence -> step.steps.forEach { collectFromStep(it, factory, registry) }
        is Execute<*> -> {
            val task = step.task as Task<WorkItemBase>
            registry.taskHandlers[task.topic] = TaskHandler(task, factory)
        }
        is Conditional -> {
            collectFromStep(step.onTrue, factory, registry)
            step.onFalse?.let { collectFromStep(it, factory, registry) }
        }
        is Loop -> collectFromStep(step.body, factory, registry)
        is Parallel -> step.branches.forEach { collectFromStep(it, factory, registry) }
        is FanOut<*, *> -> {
            val fanOut = step as FanOut<WorkItemBase, WorkItemBase>
            registry.expandHandlers["expand_${fanOut.id}"] =
                ExpandHandler(fanOut.id, fanOut.expand, factory)
        }
    }
}

private fun gather(sub: SubFlow<*>, acc: MutableList<SubFlow<*>>) {
    acc += sub
    walkFanOuts(sub.step) { gather(it.body, acc) }
}

private fun walkFanOuts(step: Step<*>, action: (FanOut<*, *>) -> Unit) {
    when (step) {
        is Sequence -> step.steps.forEach { walkFanOuts(it, action) }
        is Conditional -> {
            walkFanOuts(step.onTrue, action)
            step.onFalse?.let { walkFanOuts(it, action) }
        }
        is Loop -> walkFanOuts(step.body, action)
        is Parallel -> step.branches.forEach { walkFanOuts(it, action) }
        is FanOut<*, *> -> action(step)
        is Execute -> {}
    }
}

internal class Camunda7Run(
    private val workflow: Workflow<*>,
    private val config: RunConfig,
    private val client: Camunda7Client,
    private val registry: Registry,
    private val levelKeys: List<String>,
) : RunHandle, RunInspector {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val workerId = "loadshift-${UUID.randomUUID()}"
    private val startSignal = CompletableDeferred<Unit>()
    private val completion = CompletableDeferred<RunResult>()

    private val done = AtomicLong()
    private val failed = AtomicLong()
    private val seeded = AtomicLong()

    @Volatile private var running = true
    @Volatile private var runState = if (config.start is Start.Now) RunState.Running else RunState.Scheduled
    private var workers: List<Job> = emptyList()

    fun begin() {
        workers = (1..config.maxConcurrency).map { scope.launch { workerLoop() } }
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
                startRootInstances()
                awaitDrain()
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

    override fun state(): RunState = runState

    override suspend fun engineActive(): Long? =
        runCatching { levelKeys.sumOf { client.processInstanceCount(it) } }.getOrNull()

    private suspend fun startRootInstances() {
        val semaphore = Semaphore(config.maxConcurrency)
        coroutineScope {
            workflow.seed().toList().forEachIndexed { index, item ->
                seeded.incrementAndGet()
                semaphore.acquire()
                launch {
                    try {
                        client.startInstance(
                            processDefinitionKey = workflow.root.key,
                            variables = CamundaVariables.toCamunda(item.toMap()),
                            businessKey = index.toString(),
                        )
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
            val total = levelKeys.sumOf { client.processInstanceCount(it) }
            if (total == 0L) {
                if (++emptyPolls >= 3) return
            } else {
                emptyPolls = 0
            }
        }
    }

    private suspend fun workerLoop() {
        val topics = registry.topics.map {
            FetchTopicDto(topicName = it, lockDuration = config.lockDuration.inWholeMilliseconds)
        }
        if (topics.isEmpty()) return
        var idle = 200L
        while (running) {
            val tasks = try {
                client.fetchAndLock(FetchAndLockRequest(workerId = workerId, maxTasks = 10, topics = topics))
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                delay(1000)
                continue
            }
            if (tasks.isEmpty()) {
                delay(idle)
                idle = (idle * 2).coerceAtMost(2000)
                continue
            }
            idle = 200L
            for (task in tasks) process(task)
        }
    }

    private suspend fun process(task: ExternalTaskDto) {
        val variables = unwrapItemVariables(CamundaVariables.fromCamunda(task.variables))
        try {
            val completionVars = dispatch(task.topicName, variables)
            client.complete(task.id, CompleteRequest(workerId, completionVars))
            done.incrementAndGet()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            failed.incrementAndGet()
            runCatching {
                client.failure(
                    task.id,
                    FailureRequest(
                        workerId = workerId,
                        errorMessage = e.message ?: "task failed",
                        errorDetails = e.stackTraceToString(),
                        retries = (config.retry.maxAttempts - 1).coerceAtLeast(0),
                        retryTimeout = config.retry.baseDelay.inWholeMilliseconds,
                    ),
                )
            }
        }
    }

    private suspend fun dispatch(topic: String, variables: MutableMap<String, Any?>): Map<String, CamundaValue> {
        registry.taskHandlers[topic]?.let { handler ->
            val item = handler.factory(variables)
            handler.task.execute(item)
            return CamundaVariables.toCamunda(item.toMap())
        }
        registry.decisionHandlers[topic]?.let { handler ->
            val item = handler.factory(variables)
            val result = handler.predicate(item)
            return mapOf("${handler.id}_result" to CamundaVariables.encode(result))
        }
        registry.expandHandlers[topic]?.let { handler ->
            val item = handler.factory(variables)
            val children = handler.expand(item).toList()
            return mapOf("${handler.id}_items" to CamundaVariables.encode(children.map { it.toMap() }))
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
