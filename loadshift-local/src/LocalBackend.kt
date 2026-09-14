package loadshift.local

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import loadshift.core.AwaitMessage
import loadshift.core.AwaitSignal
import loadshift.core.Call
import loadshift.core.HumanTask
import loadshift.core.UserTask
import loadshift.core.calledWorkflows
import kotlinx.coroutines.NonCancellable
import loadshift.core.Conditional
import loadshift.core.ControllableBackend
import loadshift.core.CronSchedule
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import loadshift.core.DeadLetter
import loadshift.core.DeadLetterRecord
import loadshift.core.EngineNames
import loadshift.core.WorkItemCodec
import loadshift.core.FinishedItems
import loadshift.core.ItemState
import loadshift.core.ItemStatus
import loadshift.core.WorkflowLevel
import loadshift.core.requireRequeueable
import kotlinx.coroutines.Job
import loadshift.core.ErrorPolicy
import loadshift.core.Execute
import loadshift.core.ExecutionContext
import loadshift.core.FanIn
import loadshift.core.FanOut
import loadshift.core.Loop
import loadshift.core.Parallel
import loadshift.core.ParentItemStack
import loadshift.core.Progress
import loadshift.core.RateLimiter
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.RunHandle
import loadshift.core.RunInspector
import loadshift.core.RunMetrics
import loadshift.core.RunRegistry
import loadshift.core.RunResult
import loadshift.core.RunState
import loadshift.core.RunTracker
import loadshift.core.Sequence
import loadshift.core.Start
import loadshift.core.Step
import loadshift.core.SubFlow
import loadshift.core.Task
import loadshift.core.TaskOptions
import loadshift.core.Timeout
import loadshift.core.Wait
import loadshift.core.WorkItem
import loadshift.core.Workflow
import loadshift.core.awaitNext
import loadshift.core.backoff
import loadshift.core.withTaskTimeout
import java.util.Collections
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.TimeSource

class LocalBackend(registry: RunRegistry? = null) : ControllableBackend {
    override val control = RunTracker("local", registry = registry)
    private val active: MutableSet<LocalRun<*>> = ConcurrentHashMap.newKeySet()

    override suspend fun <W : WorkItem> run(workflow: Workflow<W>, config: RunConfig): RunHandle =
        register(workflow, LocalRun(workflow, config))

    override suspend fun signal(name: String) {
        for (run in active.toList()) run.signal(name)
    }

    override suspend fun userTasks(workflow: Workflow<*>): List<UserTask> {
        val owners = (listOf(workflow) + workflow.calledWorkflows()).map { it.key }.toSet()
        return active.toList().flatMap { it.userTasks() }.filter { it.workflowKey in owners }
    }

    override suspend fun completeUserTask(taskId: String, form: JsonObject): Boolean =
        active.toList().any { it.completeUserTask(taskId, form) }

    private fun <W : WorkItem> register(workflow: Workflow<W>, run: LocalRun<W>): RunHandle {
        active += run
        run.onFinish { active -= run }
        control.track(workflow, run, run)
        return run
    }

    override suspend fun <W : WorkItem> requeue(
        workflow: Workflow<W>,
        records: List<DeadLetterRecord>,
        config: RunConfig,
    ): RunHandle {
        val levels = workflow.requireRequeueable(records, config)
        return register(workflow, LocalRun(workflow, config, Requeue(records, levels)))
    }

    suspend fun <W : WorkItem> dryRun(workflow: Workflow<W>): List<String> {
        val run = LocalRun(workflow, RunConfig(dryRun = true, maxConcurrency = 1))
        run.await()
        return run.trace()
    }
}

private class Requeue(val records: List<DeadLetterRecord>, val levels: Map<String, WorkflowLevel>)

private sealed class UnitSignal(val topic: String, val reason: String) : Exception(reason)
private class DeadLetterSignal(topic: String, reason: String) : UnitSignal(topic, reason)
private class SkipSignal(topic: String) : UnitSignal(topic, "skipped")
private class RunFailure(cause: Throwable) : Exception(cause)

private fun Throwable.unwrapRunFailure(): Throwable {
    var error = this
    while (error is RunFailure) error = error.cause ?: return error
    return error
}

private class Compensation(val topic: String, val item: WorkItem, val action: suspend () -> Unit)

private class Level(
    val workflowKey: String,
    val levelKey: String,
    val itemVariable: String?,
    val codec: WorkItemCodec<WorkItem>,
    val parents: List<Pair<WorkItem, WorkItemCodec<WorkItem>>>,
) : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<Level>
    override val key get() = Key

    fun snapshot(item: WorkItem): JsonObject {
        val extras = buildMap {
            put(EngineNames.ITEM_KEY, JsonPrimitive(item.key.orEmpty()))
            if (parents.isNotEmpty()) {
                val lineage = JsonArray(parents.map { (parent, parentCodec) -> parentCodec.encode(parent) })
                put(EngineNames.PARENTS, JsonPrimitive(lineage.toString()))
            }
        }
        return JsonObject(codec.encode(item) + extras)
    }

    fun childOf(item: WorkItem, body: SubFlow<*>, fanId: String): Level {
        @Suppress("UNCHECKED_CAST")
        val childCodec = body.codec as WorkItemCodec<WorkItem>
        return Level(workflowKey, body.key, EngineNames.item(fanId), childCodec, listOf(item to codec) + parents)
    }
}

private class CompensationStack(val actions: MutableList<Compensation>) : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<CompensationStack>
    override val key get() = Key
}

private class ItemTracker {
    @Volatile var state = ItemState.Waiting
    @Volatile var topic: String? = null
    @Volatile var job: Job? = null
    @Volatile var cancelRequested = false
}

private class Owner(val workflowKey: String) : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<Owner>
    override val key get() = Key
}

private class PendingUserTask(val task: UserTask, val form: CompletableDeferred<JsonObject>)

private class TopItem(val tracker: ItemTracker) : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<TopItem>
    override val key get() = Key
}

private class LocalRun<W : WorkItem>(
    private val workflow: Workflow<W>,
    private val config: RunConfig,
    private val requeue: Requeue? = null,
) : RunHandle, RunInspector {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val runId = UUID.randomUUID().toString()
    private val startSignal = CompletableDeferred<Unit>()
    private val completion = CompletableDeferred<RunResult>()
    private val paused = MutableStateFlow(false)
    private val stateLock = Any()

    private val counters = RunMetrics(config.metrics, workflow.key)
    private val deadLetters = Collections.synchronizedList(mutableListOf<DeadLetter>())
    private val traceTopics = Collections.synchronizedList(mutableListOf<String>())
    private val activeItems = ConcurrentHashMap<String, ItemTracker>()
    private val finishedItems = FinishedItems()

    private val globalLimiter = config.rateLimit?.let { RateLimiter(it) }
    private val taskLimiters = ConcurrentHashMap<String, RateLimiter>()

    private val waiters = mutableMapOf<Pair<String, String?>, MutableList<CompletableDeferred<Unit>>>()
    private val delivered = mutableSetOf<Pair<String, String?>>()
    private val broadcasted = mutableSetOf<String>()
    private val signalWaiters = mutableMapOf<String, MutableList<CompletableDeferred<Unit>>>()
    private val openUserTasks = ConcurrentHashMap<String, PendingUserTask>()
    private val waitersMutex = Mutex()

    @Volatile private var runState = if (config.start is Start.Now) RunState.Running else RunState.Scheduled

    init {
        scope.launch { lifecycle() }
    }

    private suspend fun lifecycle() {
        try {
            when (val start = config.start) {
                Start.Manual -> startSignal.await()
                is Start.At -> delay(start.time - Clock.System.now())
                else -> Unit
            }
            synchronized(stateLock) {
                if (runState == RunState.Scheduled) runState = if (paused.value) RunState.Paused else RunState.Running
            }
            val cron = config.start as? Start.Cron
            do {
                execute()
                if (cron != null) CronSchedule.awaitNext(cron.expr, cron.zone)
            } while (cron != null)
            finish(RunState.Completed)
        } catch (e: CancellationException) {
            finish(RunState.Cancelled)
        } catch (e: Throwable) {
            finishExceptionally(e.unwrapRunFailure())
        }
    }

    override suspend fun start() {
        startSignal.complete(Unit)
    }

    override fun progress(): Progress = counters.progress()

    override suspend fun pause() {
        paused.value = true
        transition(RunState.Running, RunState.Paused)
    }

    override suspend fun resume() {
        paused.value = false
        transition(RunState.Paused, RunState.Running)
    }

    override suspend fun cancel() {
        scope.cancel()
        finish(RunState.Cancelled)
    }

    override suspend fun detach() = cancel()

    override suspend fun item(key: String): ItemStatus? {
        val tracker = activeItems[key]
        val state = tracker?.state ?: finishedItems[key] ?: return null
        return ItemStatus(key, state, tracker?.topic, deadLetters().filter { it.key == key })
    }

    override suspend fun cancelItem(key: String): Boolean {
        val tracker = activeItems[key] ?: return false
        tracker.cancelRequested = true
        return when (tracker.state) {
            ItemState.Running -> {
                tracker.job?.cancel()
                true
            }
            ItemState.Waiting -> true
            else -> false
        }
    }

    override suspend fun await(): RunResult = completion.await()

    override suspend fun send(message: String, key: String) {
        val list = waitersMutex.withLock {
            val l = waiters.remove(message to key)
            if (l.isNullOrEmpty()) delivered.add(message to key)
            l
        }
        list?.forEach { it.complete(Unit) }
    }

    override suspend fun broadcast(message: String) {
        val lists = waitersMutex.withLock {
            broadcasted.add(message)
            waiters.keys.filter { it.first == message }.mapNotNull { waiters.remove(it) }
        }
        lists.flatten().forEach { it.complete(Unit) }
    }

    suspend fun signal(name: String) {
        val released = waitersMutex.withLock { signalWaiters.remove(name) }
        released?.forEach { it.complete(Unit) }
    }

    fun userTasks(): List<UserTask> = openUserTasks.values.map { it.task }

    fun completeUserTask(taskId: String, form: JsonObject): Boolean =
        openUserTasks.remove(taskId)?.form?.complete(form) ?: false

    fun onFinish(action: () -> Unit) {
        completion.invokeOnCompletion { action() }
    }

    override fun state(): RunState = runState

    override fun deadLetters(): List<DeadLetter> = synchronized(deadLetters) { deadLetters.toList() }

    override fun runId(): String = runId

    fun trace(): List<String> = synchronized(traceTopics) { traceTopics.toList() }

    private fun snapshotResult(): RunResult = counters.result(deadLetters())

    private fun transition(from: RunState, to: RunState) {
        synchronized(stateLock) { if (runState == from) runState = to }
    }

    private fun finish(state: RunState): Boolean = synchronized(stateLock) {
        if (completion.isCompleted) return false
        runState = state
        completion.complete(snapshotResult())
    }

    private fun finishExceptionally(cause: Throwable): Boolean = synchronized(stateLock) {
        if (completion.isCompleted) return false
        runState = RunState.Failed
        completion.completeExceptionally(cause)
    }

    private suspend fun executeRequeue(requeue: Requeue) {
        val semaphore = Semaphore(minOf(config.maxConcurrency, config.maxInFlight ?: config.maxConcurrency))
        coroutineScope {
            for (record in requeue.records) {
                counters.seeded()
                semaphore.acquire()
                launch {
                    try {
                        val level = requeue.levels.getValue(record.level)
                        if (level.itemVariable == null) {
                            @Suppress("UNCHECKED_CAST")
                            runTopItem(level.codec.decode(record.item) as W)
                        } else {
                            runRequeuedChild(level, record)
                        }
                        config.deadLetters?.remove(record.id)
                    } finally {
                        semaphore.release()
                    }
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun runRequeuedChild(level: WorkflowLevel, record: DeadLetterRecord) {
        val codec = level.codec as WorkItemCodec<WorkItem>
        val child = codec.decode(record.item)
        val lineage = (record.item[EngineNames.PARENTS] as? JsonPrimitive)?.contentOrNull
            ?.let { Json.parseToJsonElement(it).jsonArray }
            ?: JsonArray(emptyList())
        val ancestorCodecs = level.ancestorCodecs.map { it as WorkItemCodec<WorkItem> }
        val parents = ancestorCodecs.mapIndexedNotNull { index, parentCodec ->
            lineage.getOrNull(index)?.jsonObject?.let(parentCodec.decode)
        }
        val execution = ExecutionContext(runId, workflow.name, config.logSink, parents.asReversed().map { it.key ?: "?" }, child.key)
        val context = execution + ParentItemStack(parents)
        val recording = config.deadLetters?.let { Level(workflow.key, level.key, level.itemVariable, codec, parents.zip(ancestorCodecs)) }
        val completed = withContext(if (recording == null) context else context + recording) { runChild(level.step, child) }
        if (completed) counters.done()
    }

    private suspend fun execute() {
        requeue?.let { return executeRequeue(it) }
        val semaphore = Semaphore(minOf(config.maxConcurrency, config.maxInFlight ?: config.maxConcurrency))
        val seenKeys = if (config.dedupe) HashSet<String>() else null
        coroutineScope {
            workflow.seed().collect { item ->
                val key = item.key
                if (key != null && seenKeys != null && !seenKeys.add(key)) {
                    counters.skipped()
                    return@collect
                }
                if (key != null && config.resume && config.checkpoints?.isComplete(workflow.key, key) == true) {
                    counters.skipped()
                    return@collect
                }
                counters.seeded()
                val tracker = key?.let { ItemTracker().also { created -> activeItems[it] = created } }
                semaphore.acquire()
                val job = launch {
                    try {
                        runTopItem(item, tracker)
                    } finally {
                        semaphore.release()
                    }
                }
                tracker?.job = job
            }
        }
    }

    private suspend fun runTopItem(item: W, tracker: ItemTracker? = null) {
        val key = item.key
        if (tracker != null) {
            tracker.state = ItemState.Running
            if (tracker.cancelRequested) {
                counters.cancelled()
                finishItem(key, tracker, ItemState.Cancelled)
                return
            }
        }
        val ctx = ExecutionContext(runId, workflow.name, config.logSink, itemKey = key)
        val comps = Collections.synchronizedList(mutableListOf<Compensation>())
        @Suppress("UNCHECKED_CAST")
        val level = config.deadLetters?.let {
            Level(workflow.key, workflow.root.key, null, workflow.root.codec as WorkItemCodec<WorkItem>, emptyList())
        }
        var context: CoroutineContext = ctx + CompensationStack(comps)
        if (level != null) context += level
        if (tracker != null) context += TopItem(tracker)
        withContext(context) {
            try {
                interpret(workflow.root.step, item)
                counters.done()
                key?.let { config.checkpoints?.markComplete(workflow.key, it) }
                tracker?.let { finishItem(key, it, ItemState.Done) }
            } catch (e: DeadLetterSignal) {
                recordDeadLetter(item, DeadLetter(key, e.topic, e.reason))
                runCompensations(comps)
                tracker?.let { finishItem(key, it, ItemState.DeadLettered) }
            } catch (e: SkipSignal) {
                counters.skipped()
                tracker?.let { finishItem(key, it, ItemState.Skipped) }
            } catch (e: CancellationException) {
                if (tracker != null && tracker.cancelRequested) {
                    counters.cancelled()
                    finishItem(key, tracker, ItemState.Cancelled)
                }
                throw e
            }
        }
    }

    private fun finishItem(key: String?, tracker: ItemTracker, state: ItemState) {
        tracker.state = state
        if (key != null) {
            activeItems.remove(key, tracker)
            finishedItems.record(key, state)
        }
    }

    private suspend fun runChild(step: Step<*>, child: WorkItem): Boolean {
        val comps = Collections.synchronizedList(mutableListOf<Compensation>())
        return withContext(currentCoroutineContext() + CompensationStack(comps)) {
            try {
                interpret(step, child)
                true
            } catch (e: DeadLetterSignal) {
                recordDeadLetter(child, DeadLetter(child.key, e.topic, e.reason))
                runCompensations(comps)
                false
            } catch (e: SkipSignal) {
                counters.skipped()
                false
            }
        }
    }

    private suspend fun runCompensations(comps: List<Compensation>) {
        for (compensation in synchronized(comps) { comps.toList() }.asReversed()) {
            try {
                compensation.action()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                val letter = DeadLetter(compensation.item.key, EngineNames.compensate(compensation.topic), e.message ?: e.toString())
                recordDeadLetter(compensation.item, letter)
            }
        }
    }

    private suspend fun recordDeadLetter(item: WorkItem, letter: DeadLetter) {
        deadLetters += letter
        counters.deadLetter(letter.topic)
        val store = config.deadLetters ?: return
        val level = currentCoroutineContext()[Level.Key] ?: return
        store.record(
            DeadLetterRecord(
                id = UUID.randomUUID().toString(),
                workflowKey = level.workflowKey,
                level = level.levelKey,
                itemVariable = level.itemVariable,
                deadLetter = letter,
                item = level.snapshot(item),
                recordedAt = Clock.System.now(),
                runId = runId,
            ),
        )
    }

    private suspend fun currentExecutionContext(): ExecutionContext =
        currentCoroutineContext()[ExecutionContext.Key] ?: ExecutionContext(runId, workflow.name, config.logSink)

    @Suppress("UNCHECKED_CAST")
    private suspend fun interpret(step: Step<*>, item: WorkItem) {
        when (step) {
            is Sequence<*> -> for (s in step.steps) interpret(s, item)

            is Execute<*> -> {
                val e = step as Execute<WorkItem>
                config.tracer.span("task ${e.task.topic}", mapOf("item" to item.key.orEmpty())) {
                    withContext(currentExecutionContext().withTopic(e.task.topic)) {
                        runTask(e.task, e.options, item)
                    }
                }
                e.compensation?.let { comp ->
                    currentCoroutineContext()[CompensationStack.Key]?.actions?.add(Compensation(e.task.topic, item) { comp(item) })
                }
            }

            is Conditional<*> -> {
                val c = step as Conditional<WorkItem>
                val matched = guarded(EngineNames.decision(c.id)) { c.predicate(item) }
                if (matched) interpret(c.onTrue, item) else c.onFalse?.let { interpret(it, item) }
            }

            is Loop<*> -> {
                val l = step as Loop<WorkItem>
                var iterations = 0
                while (guarded(EngineNames.decision(l.id)) { l.predicate(item) }) {
                    if (++iterations > config.maxLoopIterations) {
                        throw DeadLetterSignal(EngineNames.loop(l.id), "exceeded maxLoopIterations=${config.maxLoopIterations}")
                    }
                    interpret(l.body, item)
                }
            }

            is Parallel<*> -> coroutineScope {
                step.branches.map { branch -> async { interpret(branch, item) } }.awaitAll()
            }

            is Wait<*> -> delay(step.duration)

            is AwaitMessage<*> -> {
                val k = step.message to item.key
                val signal = CompletableDeferred<Unit>()
                val resumeNow = waitersMutex.withLock {
                    if (k in delivered || step.message in broadcasted) {
                        delivered.remove(k)
                        true
                    } else {
                        waiters.getOrPut(k) { mutableListOf() }.add(signal)
                        false
                    }
                }
                if (!resumeNow) signal.await()
            }

            is HumanTask<*> -> {
                val human = step as HumanTask<WorkItem>
                val owner = currentCoroutineContext()[Owner.Key]?.workflowKey ?: workflow.key
                val pending = PendingUserTask(
                    UserTask(UUID.randomUUID().toString(), owner, human.name, item.key, human.assignee, human.candidateGroups),
                    CompletableDeferred(),
                )
                openUserTasks[pending.task.id] = pending
                val form = try {
                    pending.form.await()
                } finally {
                    openUserTasks.remove(pending.task.id)
                }
                val topic = EngineNames.form(human.id)
                withContext(currentExecutionContext().withTopic(topic)) {
                    try {
                        executeWithRetry(config.retry, null, topic) { human.onComplete(item, form) }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Throwable) {
                        throw failureSignal(topic, e)
                    }
                }
            }

            is Call<*> -> {
                @Suppress("UNCHECKED_CAST")
                val called = (step as Call<WorkItem>).workflow
                val comps = Collections.synchronizedList(mutableListOf<Compensation>())
                var context: CoroutineContext = ExecutionContext(runId, called.name, config.logSink, emptyList(), item.key) +
                    ParentItemStack(emptyList()) + CompensationStack(comps) + Owner(called.key)
                if (currentCoroutineContext()[Level.Key] != null) {
                    context += Level(called.key, called.root.key, null, called.root.codec, emptyList())
                }
                withContext(context) {
                    try {
                        interpret(called.root.step, item)
                    } catch (e: DeadLetterSignal) {
                        recordDeadLetter(item, DeadLetter(item.key, e.topic, e.reason))
                        runCompensations(comps)
                    } catch (e: SkipSignal) {
                        counters.skipped()
                    }
                }
            }

            is AwaitSignal<*> -> {
                val released = CompletableDeferred<Unit>()
                waitersMutex.withLock { signalWaiters.getOrPut(step.signal) { mutableListOf() }.add(released) }
                try {
                    released.await()
                } finally {
                    withContext(NonCancellable) {
                        waitersMutex.withLock {
                            signalWaiters[step.signal]?.let { waiting ->
                                waiting.remove(released)
                                if (waiting.isEmpty()) signalWaiters.remove(step.signal)
                            }
                        }
                    }
                }
            }

            is Timeout<*> -> {
                val t = step as Timeout<WorkItem>
                withTimeoutOrNull(t.duration) { interpret(t.body, item); true }
                    ?: throw DeadLetterSignal(EngineNames.timeout(t.id), "exceeded ${t.duration}")
            }

            is FanOut<*, *> -> {
                val fanOut = step as FanOut<WorkItem, WorkItem>
                fan(fanOut.id, fanOut.expand, fanOut.concurrency, fanOut.body, item) { }
            }

            is FanIn<*, *, *> -> {
                val fanIn = step as FanIn<WorkItem, WorkItem, Any?>
                val reduce = EngineNames.reduce(fanIn.id)
                var acc = fanIn.initial
                fan(fanIn.id, fanIn.expand, fanIn.concurrency, fanIn.body, item) { child ->
                    acc = guarded(reduce) { fanIn.combine(acc, child) }
                }
                guarded(reduce) { fanIn.onComplete(item, acc) }
            }
        }
    }

    private suspend fun fan(
        id: String,
        expand: suspend (WorkItem) -> Flow<WorkItem>,
        concurrency: Int?,
        body: SubFlow<*>,
        item: WorkItem,
        onChild: suspend (WorkItem) -> Unit,
    ) {
        val semaphore = Semaphore(concurrency ?: config.maxConcurrency)
        val ctx = currentExecutionContext()
        val parentStack = (currentCoroutineContext()[ParentItemStack.Key] ?: ParentItemStack(emptyList())).push(item)
        val childLevel = currentCoroutineContext()[Level.Key]?.childOf(item, body, id)
        guarded(EngineNames.expand(id)) {
            coroutineScope {
                expand(item).collect { child ->
                    counters.expanded()
                    onChild(child)
                    semaphore.acquire()
                    launch {
                        try {
                            val childContext = ctx.child(child.key ?: "?") + parentStack
                            withContext(if (childLevel == null) childContext else childContext + childLevel) {
                                runChild(body.step, child)
                            }
                        } finally {
                            semaphore.release()
                        }
                    }
                }
            }
        }
    }

    private inline fun <T> guarded(topic: String, block: () -> T): T =
        try {
            block()
        } catch (e: CancellationException) {
            throw e
        } catch (e: UnitSignal) {
            throw e
        } catch (e: RunFailure) {
            throw e
        } catch (e: Throwable) {
            throw failureSignal(topic, e)
        }

    private fun failureSignal(topic: String, error: Throwable): Throwable = when (config.onError) {
        ErrorPolicy.Fail -> {
            counters.failed()
            RunFailure(error)
        }
        ErrorPolicy.DeadLetter -> DeadLetterSignal(topic, error.message ?: error.toString())
        ErrorPolicy.Skip -> SkipSignal(topic)
    }

    private suspend fun runTask(task: Task<WorkItem>, options: TaskOptions, item: WorkItem) {
        paused.first { !it }
        currentCoroutineContext()[TopItem.Key]?.tracker?.topic = task.topic

        if (config.dryRun) {
            traceTopics += task.topic
            return
        }

        globalLimiter?.acquire()
        options.rateLimit?.let { taskLimiters.getOrPut(task.topic) { RateLimiter(it) }.acquire() }

        val policy = options.retry ?: config.retry
        val timeout = options.timeout ?: policy.timeout
        try {
            executeWithRetry(policy, timeout, task.topic) { task.execute(item) }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            throw failureSignal(task.topic, e)
        }
    }

    private suspend fun executeWithRetry(policy: RetryPolicy, timeout: Duration?, topic: String, block: suspend () -> Unit) {
        var attempt = 0
        while (true) {
            attempt++
            val started = TimeSource.Monotonic.markNow()
            try {
                withTaskTimeout(timeout, block)
                counters.taskDuration(topic, true, started.elapsedNow())
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                counters.taskDuration(topic, false, started.elapsedNow())
                if (attempt >= policy.maxAttempts || !policy.retryOn(e)) throw e
                counters.retry(topic)
                delay(policy.backoff(attempt))
            }
        }
    }
}
