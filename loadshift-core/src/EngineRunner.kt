package loadshift.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

@EngineApi
class EngineRunner(
    private val run: EngineRun,
    private val driver: EngineDriver,
    private val rootProcessId: String,
    private val processIds: List<String>,
    private val attach: Boolean = false,
    private val pollInterval: Duration = 500.milliseconds,
    private val fetchWait: Duration = 5.seconds,
) : RunHandle, RunInspector {
    private class Root(val key: String?)

    private val config = run.config

    init {
        require(!attach || config.start !is Start.Cron) { "an attached run cannot use Start.Cron" }
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val startSignal = CompletableDeferred<Unit>()
    private val completion = CompletableDeferred<RunResult>()
    private val paused = MutableStateFlow(false)
    private val stopping = MutableStateFlow(false)
    private val workSlots = Semaphore(config.maxConcurrency)
    private val pendingRoots = ConcurrentHashMap<String, Root>()
    private val finishedRoots: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val pendingSends: MutableSet<Pair<String, String>> = ConcurrentHashMap.newKeySet()
    private val broadcasts: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val pollers = mutableListOf<Job>()
    private val stateLock = Any()

    @Volatile private var runState = if (config.start is Start.Now) RunState.Running else RunState.Scheduled
    @Volatile private var ending: RunState? = null
    private lateinit var lifecycle: Job

    fun begin(): EngineRunner {
        lifecycle = scope.launch { runLifecycle() }
        return this
    }

    override suspend fun start() {
        startSignal.complete(Unit)
    }

    override suspend fun pause() {
        paused.value = true
        synchronized(stateLock) { if (runState == RunState.Running) runState = RunState.Paused }
    }

    override suspend fun resume() {
        paused.value = false
        synchronized(stateLock) { if (runState == RunState.Paused) runState = RunState.Running }
    }

    override suspend fun cancel() {
        if (completion.isCompleted) return
        ending = RunState.Cancelled
        scope.cancel()
        withContext(NonCancellable) { cancelRoots() }
        finish(RunState.Cancelled)
    }

    override suspend fun detach() {
        if (completion.isCompleted) return
        ending = RunState.Detached
        withContext(NonCancellable) {
            stopping.value = true
            lifecycle.cancelAndJoin()
            synchronized(pollers) { pollers.toList() }.joinAll()
            repeat(config.maxConcurrency) { workSlots.acquire() }
        }
        finish(RunState.Detached)
        scope.cancel()
    }

    override suspend fun await(): RunResult = completion.await()

    override suspend fun send(message: String, key: String) {
        if (!correlate(message, key)) pendingSends += message to key
    }

    override suspend fun broadcast(message: String) {
        broadcasts += message
        correlate(message, null)
    }

    override fun progress(): Progress = run.progress()

    override fun state(): RunState = runState

    override fun deadLetters(): List<DeadLetter> = run.deadLetters()

    override suspend fun engineActive(): Long? = attempt { driver.activeInstances(processIds) }

    private suspend fun runLifecycle() {
        try {
            when (val start = config.start) {
                Start.Manual -> startSignal.await()
                is Start.At -> delay(start.time - Clock.System.now())
                else -> Unit
            }
            synchronized(stateLock) {
                if (runState == RunState.Scheduled) runState = if (paused.value) RunState.Paused else RunState.Running
            }
            if (!config.dryRun) work()
            finish(RunState.Completed)
            scope.cancel()
        } catch (e: CancellationException) {
            if (ending == null && finish(RunState.Cancelled)) scope.cancel()
        } catch (e: Throwable) {
            abort(e)
        }
    }

    private suspend fun work() {
        startPollers()
        try {
            if (attach) {
                drain()
            } else {
                val cron = config.start as? Start.Cron
                do {
                    seed()
                    drain()
                    run.clearInstanceState()
                    if (cron != null) CronSchedule.awaitNext(cron.expr, cron.zone)
                } while (cron != null)
            }
        } finally {
            stopping.value = true
            withContext(NonCancellable) { synchronized(pollers) { pollers.toList() }.joinAll() }
        }
    }

    private fun startPollers() {
        if (run.jobTypes.isEmpty()) return
        val groups = driver.pollGroups(run.jobTypes)
        val wait = if (groups.size == 1) fetchWait else Duration.ZERO
        synchronized(pollers) {
            for (group in groups) pollers += scope.launch { poll(group, wait) }
        }
    }

    private suspend fun adopt(): Boolean {
        awaitRunning()
        val roots = attempt { driver.activeRoots(rootProcessId) } ?: return true
        val fresh = roots.count { it.id !in finishedRoots && pendingRoots.putIfAbsent(it.id, Root(it.itemKey)) == null }
        run.recordAttached(fresh)
        return pendingRoots.isNotEmpty()
    }

    private suspend fun seed() {
        val seen = if (config.dedupe) HashSet<String>() else null
        val starts = Semaphore(config.maxConcurrency)
        coroutineScope {
            run.workflow.seed().collect { item ->
                awaitRunning()
                if (!run.admit(item, seen)) return@collect
                starts.acquire()
                launch {
                    try {
                        val id = driver.startInstance(rootProcessId, run.rootVariables(item), item.key)
                        pendingRoots[id] = Root(item.key)
                    } finally {
                        starts.release()
                    }
                }
            }
        }
    }

    private suspend fun drain() {
        while (true) {
            deliverMessages()
            if (pendingRoots.isEmpty() && (!attach || !adopt())) return
            delay(pollInterval)
            val finished = attempt { driver.finished(pendingRoots.keys.toList()) } ?: continue
            for ((id, completed) in finished) {
                val root = pendingRoots.remove(id) ?: continue
                finishedRoots += id
                run.rootFinished(id, root.key, completed)
            }
        }
    }

    private suspend fun poll(jobTypes: List<String>, wait: Duration) {
        var idle = pollInterval / 5
        while (true) {
            combine(paused, stopping) { isPaused, isStopping -> isStopping || !isPaused }.first { it }
            if (stopping.value) return
            workSlots.acquire()
            if (stopping.value) {
                workSlots.release()
                return
            }
            var slots = 1
            while (slots < MAX_BATCH && workSlots.tryAcquire()) slots++
            val fetchedAt = TimeSource.Monotonic.markNow()
            val jobs = try {
                driver.fetch(jobTypes, slots, config.lockDuration, wait)
            } catch (e: CancellationException) {
                repeat(slots) { workSlots.release() }
                throw e
            } catch (e: Throwable) {
                repeat(slots) { workSlots.release() }
                delay(FETCH_ERROR_BACKOFF)
                continue
            }
            if (stopping.value) {
                repeat(slots) { workSlots.release() }
                for (job in jobs) attempt { driver.release(job) }
                return
            }
            val accepted = jobs.take(slots)
            repeat(slots - accepted.size) { workSlots.release() }
            for (job in accepted) {
                scope.launch {
                    try {
                        process(job, fetchedAt)
                    } finally {
                        workSlots.release()
                    }
                }
            }
            if (accepted.isEmpty() && wait == Duration.ZERO) {
                delay(idle)
                idle = (idle * 2).coerceAtMost(pollInterval * 2)
            } else {
                idle = pollInterval / 5
            }
        }
    }

    private suspend fun process(job: EngineJob, lockedAt: TimeSource.Monotonic.ValueTimeMark) {
        val heartbeat = scope.launch {
            val interval = config.lockDuration / 3
            var tick = 1
            while (true) {
                delay(interval * tick - lockedAt.elapsedNow())
                tick++
                launch { attempt { driver.extendLock(job, config.lockDuration) } }
            }
        }
        try {
            when (val outcome = run.execute(job)) {
                is JobOutcome.Complete -> deliver { driver.complete(job, outcome.variables) }
                is JobOutcome.Retry -> deliver {
                    driver.fail(job, outcome.retries, outcome.backoff, outcome.message, outcome.details)
                }
                is JobOutcome.Terminate -> deliver { driver.terminate(job, outcome.message, outcome.variables) }
                is JobOutcome.Abort -> {
                    val cause = outcome.cause
                    attempt { driver.fail(job, 0, Duration.ZERO, cause.message ?: cause.toString(), cause.stackTraceToString()) }
                    abort(cause)
                }
            }
        } finally {
            heartbeat.cancel()
        }
    }

    private suspend fun abort(cause: Throwable) {
        if (completion.isCompleted) return
        withContext(NonCancellable) { cancelRoots() }
        finishExceptionally(cause)
        scope.cancel()
    }

    private suspend fun cancelRoots() {
        for (id in pendingRoots.keys.toList()) {
            attempt { driver.cancel(id) }
            pendingRoots.remove(id)
        }
    }

    private suspend fun deliverMessages() {
        for (pending in pendingSends.toList()) {
            if (correlate(pending.first, pending.second)) pendingSends.remove(pending)
        }
        for (message in broadcasts.toList()) correlate(message, null)
    }

    private suspend fun correlate(message: String, key: String?): Boolean =
        attempt { driver.correlate(message, run.workflow.key, key) } ?: false

    private suspend fun awaitRunning() {
        paused.first { !it }
    }

    private suspend fun deliver(action: suspend () -> Unit) {
        repeat(DELIVERY_ATTEMPTS) { index ->
            if (attempt { action() } != null) return
            delay(DELIVERY_BACKOFF * (index + 1))
        }
    }

    private fun finish(state: RunState): Boolean = synchronized(stateLock) {
        if (completion.isCompleted) return false
        runState = state
        completion.complete(run.result())
    }

    private fun finishExceptionally(cause: Throwable): Boolean = synchronized(stateLock) {
        if (completion.isCompleted) return false
        runState = RunState.Failed
        completion.completeExceptionally(cause)
    }

    private suspend fun <T> attempt(block: suspend () -> T): T? =
        try {
            block()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            null
        }

    private companion object {
        const val MAX_BATCH = 10
        const val DELIVERY_ATTEMPTS = 5
        val DELIVERY_BACKOFF = 150.milliseconds
        val FETCH_ERROR_BACKOFF = 1.seconds
    }
}
