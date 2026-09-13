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
import loadshift.core.Conditional
import loadshift.core.ControllableBackend
import loadshift.core.CronSchedule
import loadshift.core.DeadLetter
import loadshift.core.EngineNames
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
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.time.Clock
import kotlin.time.Duration

class LocalBackend : ControllableBackend {
    override val control = RunTracker("local")

    override suspend fun <W : WorkItem> run(workflow: Workflow<W>, config: RunConfig): RunHandle {
        val run = LocalRun(workflow, config)
        control.track(workflow, run, run)
        return run
    }

    suspend fun <W : WorkItem> dryRun(workflow: Workflow<W>): List<String> {
        val run = LocalRun(workflow, RunConfig(dryRun = true, maxConcurrency = 1))
        run.await()
        return run.trace()
    }
}

private sealed class UnitSignal(val topic: String, val reason: String) : Exception(reason)
private class DeadLetterSignal(topic: String, reason: String) : UnitSignal(topic, reason)
private class SkipSignal(topic: String) : UnitSignal(topic, "skipped")
private class RunFailure(cause: Throwable) : Exception(cause)

private fun Throwable.unwrapRunFailure(): Throwable {
    var error = this
    while (error is RunFailure) error = error.cause ?: return error
    return error
}

private class Compensation(val topic: String, val key: String?, val action: suspend () -> Unit)

private class CompensationStack(val actions: MutableList<Compensation>) : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<CompensationStack>
    override val key get() = Key
}

private class LocalRun<W : WorkItem>(
    private val workflow: Workflow<W>,
    private val config: RunConfig,
) : RunHandle, RunInspector {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val runId = UUID.randomUUID().toString()
    private val startSignal = CompletableDeferred<Unit>()
    private val completion = CompletableDeferred<RunResult>()
    private val paused = MutableStateFlow(false)
    private val stateLock = Any()

    private val seeded = AtomicLong()
    private val expanded = AtomicLong()
    private val done = AtomicLong()
    private val failed = AtomicLong()
    private val skipped = AtomicLong()
    private val deadLetters = Collections.synchronizedList(mutableListOf<DeadLetter>())
    private val traceTopics = Collections.synchronizedList(mutableListOf<String>())

    private val globalLimiter = config.rateLimit?.let { RateLimiter(it) }
    private val taskLimiters = ConcurrentHashMap<String, RateLimiter>()

    private val waiters = mutableMapOf<Pair<String, String?>, MutableList<CompletableDeferred<Unit>>>()
    private val delivered = mutableSetOf<Pair<String, String?>>()
    private val broadcasted = mutableSetOf<String>()
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

    override fun progress(): Progress =
        Progress(seeded.get(), expanded.get(), done.get(), failed.get(), skipped.get())

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

    override fun state(): RunState = runState

    override fun deadLetters(): List<DeadLetter> = synchronized(deadLetters) { deadLetters.toList() }

    fun trace(): List<String> = synchronized(traceTopics) { traceTopics.toList() }

    private fun snapshotResult(): RunResult = RunResult(done.get(), failed.get(), skipped.get(), deadLetters())

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

    private suspend fun execute() {
        val semaphore = Semaphore(config.maxConcurrency)
        val seenKeys = if (config.dedupe) HashSet<String>() else null
        coroutineScope {
            workflow.seed().collect { item ->
                val key = item.key
                if (key != null && seenKeys != null && !seenKeys.add(key)) {
                    skipped.incrementAndGet()
                    return@collect
                }
                if (key != null && config.resume && config.checkpoints?.isComplete(workflow.key, key) == true) {
                    skipped.incrementAndGet()
                    return@collect
                }
                seeded.incrementAndGet()
                semaphore.acquire()
                launch {
                    try {
                        runTopItem(item)
                    } finally {
                        semaphore.release()
                    }
                }
            }
        }
    }

    private suspend fun runTopItem(item: W) {
        val ctx = ExecutionContext(runId, workflow.name, config.logSink, itemKey = item.key)
        val comps = Collections.synchronizedList(mutableListOf<Compensation>())
        withContext(ctx + CompensationStack(comps)) {
            try {
                interpret(workflow.root.step, item)
                done.incrementAndGet()
                item.key?.let { config.checkpoints?.markComplete(workflow.key, it) }
            } catch (e: DeadLetterSignal) {
                deadLetters += DeadLetter(item.key, e.topic, e.reason)
                runCompensations(comps)
            } catch (e: SkipSignal) {
                skipped.incrementAndGet()
            }
        }
    }

    private suspend fun runChild(step: Step<*>, child: WorkItem) {
        val comps = Collections.synchronizedList(mutableListOf<Compensation>())
        withContext(currentCoroutineContext() + CompensationStack(comps)) {
            try {
                interpret(step, child)
            } catch (e: DeadLetterSignal) {
                deadLetters += DeadLetter(child.key, e.topic, e.reason)
                runCompensations(comps)
            } catch (e: SkipSignal) {
                skipped.incrementAndGet()
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
                deadLetters += DeadLetter(compensation.key, EngineNames.compensate(compensation.topic), e.message ?: e.toString())
            }
        }
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
                    currentCoroutineContext()[CompensationStack.Key]?.actions?.add(Compensation(e.task.topic, item.key) { comp(item) })
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
        guarded(EngineNames.expand(id)) {
            coroutineScope {
                expand(item).collect { child ->
                    expanded.incrementAndGet()
                    onChild(child)
                    semaphore.acquire()
                    launch {
                        try {
                            withContext(ctx.child(child.key ?: "?") + parentStack) {
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
            failed.incrementAndGet()
            RunFailure(error)
        }
        ErrorPolicy.DeadLetter -> DeadLetterSignal(topic, error.message ?: error.toString())
        ErrorPolicy.Skip -> SkipSignal(topic)
    }

    private suspend fun runTask(task: Task<WorkItem>, options: TaskOptions, item: WorkItem) {
        paused.first { !it }

        if (config.dryRun) {
            traceTopics += task.topic
            return
        }

        globalLimiter?.acquire()
        options.rateLimit?.let { taskLimiters.getOrPut(task.topic) { RateLimiter(it) }.acquire() }

        val policy = options.retry ?: config.retry
        val timeout = options.timeout ?: policy.timeout
        try {
            executeWithRetry(policy, timeout) { task.execute(item) }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            throw failureSignal(task.topic, e)
        }
    }

    private suspend fun executeWithRetry(policy: RetryPolicy, timeout: Duration?, block: suspend () -> Unit) {
        var attempt = 0
        while (true) {
            attempt++
            try {
                withTaskTimeout(timeout, block)
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                if (attempt >= policy.maxAttempts || !policy.retryOn(e)) throw e
                delay(policy.backoff(attempt))
            }
        }
    }
}
