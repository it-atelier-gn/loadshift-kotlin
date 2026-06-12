package loadshift.local

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeout
import kotlinx.datetime.Clock
import loadshift.core.Conditional
import loadshift.core.DeadLetter
import loadshift.core.ErrorPolicy
import loadshift.core.Execute
import loadshift.core.FanOut
import loadshift.core.IntrospectableBackend
import loadshift.core.Loop
import loadshift.core.Parallel
import loadshift.core.Progress
import loadshift.core.Rate
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
import loadshift.core.Task
import loadshift.core.TaskOptions
import loadshift.core.WorkItemBase
import loadshift.core.Workflow
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random
import kotlin.time.Duration

class LocalBackend : IntrospectableBackend {
    override val introspection = RunTracker("local")

    override suspend fun <W : WorkItemBase> run(workflow: Workflow<W>, config: RunConfig): RunHandle {
        val run = LocalRun(workflow, config)
        introspection.track(workflow, run)
        return run
    }

    suspend fun <W : WorkItemBase> dryRun(workflow: Workflow<W>): List<String> {
        val run = LocalRun(workflow, RunConfig(dryRun = true, maxConcurrency = 1))
        run.await()
        return run.trace()
    }
}

private class RateLimiter(rate: Rate) {
    private val intervalNanos = rate.per.inWholeNanoseconds / rate.permits
    private val lock = Any()
    private var next = 0L

    suspend fun acquire() {
        val waitNanos = synchronized(lock) {
            val now = System.nanoTime()
            val at = maxOf(now, next)
            next = at + intervalNanos
            at - now
        }
        if (waitNanos > 0) delay(waitNanos / 1_000_000)
    }
}

private sealed class UnitSignal(val topic: String, val reason: String) : Exception()
private class DeadLetterSignal(topic: String, reason: String) : UnitSignal(topic, reason)
private class SkipSignal(topic: String) : UnitSignal(topic, "skipped")

private class LocalRun<W : WorkItemBase>(
    private val workflow: Workflow<W>,
    private val config: RunConfig,
) : RunHandle, RunInspector {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val startSignal = CompletableDeferred<Unit>()
    private val completion = CompletableDeferred<RunResult>()

    private val seeded = AtomicLong()
    private val expanded = AtomicLong()
    private val done = AtomicLong()
    private val failed = AtomicLong()
    private val skipped = AtomicLong()
    private val deadLetters = Collections.synchronizedList(mutableListOf<DeadLetter>())
    private val traceTopics = Collections.synchronizedList(mutableListOf<String>())

    private val globalLimiter = config.rateLimit?.let { RateLimiter(it) }
    private val taskLimiters = ConcurrentHashMap<String, RateLimiter>()

    @Volatile private var paused = false
    @Volatile private var runState = if (config.start is Start.Now) RunState.Running else RunState.Scheduled

    init {
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
                execute()
                runState = RunState.Completed
                completion.complete(snapshotResult())
            } catch (e: CancellationException) {
                runState = RunState.Cancelled
                completion.complete(snapshotResult())
            } catch (e: Throwable) {
                runState = RunState.Failed
                completion.completeExceptionally(e)
            }
        }
    }

    override suspend fun start() {
        startSignal.complete(Unit)
    }

    override fun progress(): Progress =
        Progress(seeded.get(), expanded.get(), done.get(), failed.get(), skipped.get())

    override suspend fun pause() {
        paused = true
        if (runState == RunState.Running) runState = RunState.Paused
    }

    override suspend fun cancel() {
        runState = RunState.Cancelled
        scope.cancel()
        if (!completion.isCompleted) completion.complete(snapshotResult())
    }

    override suspend fun await(): RunResult = completion.await()

    override fun state(): RunState = runState

    override fun deadLetters(): List<DeadLetter> = synchronized(deadLetters) { deadLetters.toList() }

    fun trace(): List<String> = synchronized(traceTopics) { traceTopics.toList() }

    private fun snapshotResult(): RunResult =
        RunResult(done.get(), failed.get(), skipped.get(), synchronized(deadLetters) { deadLetters.toList() })

    private suspend fun execute() {
        val semaphore = Semaphore(config.maxConcurrency)
        coroutineScope {
            workflow.seed().collect { item ->
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
        try {
            interpret(workflow.root.step, item)
            done.incrementAndGet()
        } catch (e: DeadLetterSignal) {
            deadLetters += DeadLetter(item.key, e.topic, e.reason)
            failed.incrementAndGet()
        } catch (e: SkipSignal) {
            skipped.incrementAndGet()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun interpret(step: Step<*>, item: WorkItemBase) {
        when (step) {
            is Sequence<*> -> for (s in step.steps) interpret(s, item)

            is Execute<*> -> runTask((step as Execute<WorkItemBase>).task, step.options, item)

            is Conditional<*> -> {
                val c = step as Conditional<WorkItemBase>
                if (c.predicate(item)) interpret(c.onTrue, item) else c.onFalse?.let { interpret(it, item) }
            }

            is Loop<*> -> {
                val l = step as Loop<WorkItemBase>
                var iterations = 0
                while (l.predicate(item)) {
                    if (++iterations > config.maxLoopIterations) {
                        throw DeadLetterSignal("loop_${l.id}", "exceeded maxLoopIterations=${config.maxLoopIterations}")
                    }
                    interpret(l.body, item)
                }
            }

            is Parallel<*> -> coroutineScope {
                step.branches.map { branch -> async { interpret(branch, item) } }.awaitAll()
            }

            is FanOut<*, *> -> {
                val fanOut = step as FanOut<WorkItemBase, WorkItemBase>
                val childFlow = fanOut.expand(item)
                val semaphore = Semaphore(fanOut.concurrency ?: config.maxConcurrency)
                coroutineScope {
                    childFlow.collect { child ->
                        expanded.incrementAndGet()
                        semaphore.acquire()
                        launch {
                            try {
                                runChild(fanOut.body.step, child)
                            } finally {
                                semaphore.release()
                            }
                        }
                    }
                }
            }
        }
    }

    private suspend fun runChild(step: Step<*>, child: WorkItemBase) {
        try {
            interpret(step, child)
        } catch (e: DeadLetterSignal) {
            deadLetters += DeadLetter(child.key, e.topic, e.reason)
            failed.incrementAndGet()
        } catch (e: SkipSignal) {
            skipped.incrementAndGet()
        }
    }

    private suspend fun runTask(task: Task<WorkItemBase>, options: TaskOptions, item: WorkItemBase) {
        while (paused) delay(50)

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
            when (config.onError) {
                ErrorPolicy.Fail -> throw e
                ErrorPolicy.DeadLetter -> throw DeadLetterSignal(task.topic, e.message ?: e.toString())
                ErrorPolicy.Skip -> throw SkipSignal(task.topic)
            }
        }
    }

    private suspend fun executeWithRetry(policy: RetryPolicy, timeout: Duration?, block: suspend () -> Unit) {
        var attempt = 0
        while (true) {
            attempt++
            try {
                if (timeout != null) withTimeout(timeout) { block() } else block()
                return
            } catch (e: TimeoutCancellationException) {
                if (attempt >= policy.maxAttempts) throw e
                delay(backoffMillis(policy, attempt))
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                if (attempt >= policy.maxAttempts || !policy.retryOn(e)) throw e
                delay(backoffMillis(policy, attempt))
            }
        }
    }

    private fun backoffMillis(policy: RetryPolicy, attempt: Int): Long {
        val base = policy.baseDelay.inWholeMilliseconds
        val raw = base shl (attempt - 1).coerceAtMost(20)
        val capped = minOf(raw, policy.maxDelay.inWholeMilliseconds)
        return if (policy.jitter) (capped * Random.nextDouble(0.5, 1.0)).toLong() else capped
    }
}
