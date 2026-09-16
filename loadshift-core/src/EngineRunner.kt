package loadshift.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
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
    private val requeue: List<DeadLetterRecord>? = null,
    private val scheduleProcessId: String? = null,
    private val pollInterval: Duration = 500.milliseconds,
    private val fetchWait: Duration = 5.seconds,
) : RunHandle, RunInspector {
    private class Root(val key: String?, val holdsPermit: Boolean)

    private val config = run.config

    init {
        require(!attach || config.start !is Start.Cron) { "an attached run cannot use Start.Cron" }
        require(!attach || requeue == null) { "a run either attaches or requeues" }
        require((config.start is Start.Cron) == (scheduleProcessId != null)) {
            "a schedule process is required for Start.Cron and allowed only with it"
        }
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val startSignal = CompletableDeferred<Unit>()
    private val completion = CompletableDeferred<RunResult>()
    private val paused = MutableStateFlow(false)
    private val stopping = MutableStateFlow(false)
    private val workSlots = Semaphore(config.maxConcurrency)
    private val inFlight = config.maxInFlight?.let { Semaphore(it) }
    private val nextTickType = EngineNames.jobType(run.workflow.key, EngineNames.SCHEDULE_NEXT)
    private val seedType = EngineNames.jobType(run.workflow.key, EngineNames.SCHEDULE_SEED)
    private val awaitType = EngineNames.jobType(run.workflow.key, EngineNames.SCHEDULE_AWAIT)
    @Volatile private var scheduler: String? = null
    private val pendingRoots = ConcurrentHashMap<String, Root>()
    private val rootsByKey = ConcurrentHashMap<String, String>()
    private val finishedItems = FinishedItems()
    private val finishedRoots: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val executing: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val pendingSends: MutableSet<Pair<String, String>> = ConcurrentHashMap.newKeySet()
    private val broadcasts: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val pollers = mutableListOf<Job>()
    private val stateLock = Any()

    @Volatile private var runState = if (config.start is Start.Now) RunState.Running else RunState.Scheduled
    @Volatile private var ending: RunState? = null
    @Volatile private var halting = false
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
        halting = true
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

    override suspend fun item(key: String): ItemStatus? {
        val letters = run.deadLetters().filter { it.key == key }
        val id = rootsByKey[key]
        if (id != null && pendingRoots.containsKey(id)) return ItemStatus(key, ItemState.Running, null, letters)
        val state = finishedItems[key] ?: return null
        return ItemStatus(key, state, null, letters)
    }

    override suspend fun cancelItem(key: String): Boolean {
        val id = rootsByKey[key] ?: return false
        val root = pendingRoots.remove(id) ?: return false
        if (attempt { driver.cancel(id) } == null) {
            pendingRoots[id] = root
            return false
        }
        finishedRoots += id
        rootsByKey.remove(key, id)
        releaseInFlight(root)
        run.recordCancelled()
        finishedItems.record(key, ItemState.Cancelled)
        return true
    }

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

    override fun runId(): String = run.runId

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
        } finally {
            withContext(NonCancellable) { attempt { driver.close() } }
        }
    }

    private suspend fun work() {
        startPollers()
        try {
            val records = requeue
            if (attach) {
                drain(null)
            } else if (records != null) {
                coroutineScope { drain(launch { startRequeued(records) }) }
            } else if (scheduleProcessId != null) {
                adopt()
                startScheduler(scheduleProcessId)
                drain(null)
            } else {
                coroutineScope { drain(launch { seed() }) }
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

    private suspend fun startScheduler(processId: String) {
        awaitRunning()
        scheduler = driver.activeRoots(processId).minByOrNull { it.id }?.id ?: startSchedulerInstance(processId)
        synchronized(pollers) { pollers += scope.launch { pollSchedule() } }
    }

    private suspend fun pollSchedule() {
        val types = listOf(nextTickType, seedType, awaitType)
        while (true) {
            combine(paused, stopping) { isPaused, isStopping -> isStopping || !isPaused }.first { it }
            if (stopping.value) return
            val fetchedAt = TimeSource.Monotonic.markNow()
            val jobs = try {
                driver.fetch(types, 1, config.lockDuration, fetchWait)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                delay(FETCH_ERROR_BACKOFF)
                continue
            }
            if (jobs.isEmpty()) delay(pollInterval)
            for (job in jobs) {
                if (stopping.value) attempt { driver.release(job) } else runScheduleJob(job, fetchedAt)
            }
        }
    }

    private suspend fun runScheduleJob(job: EngineJob, lockedAt: TimeSource.Monotonic.ValueTimeMark) = supervisorScope {
        val heartbeat = heartbeat(job, lockedAt)
        try {
            val work = async {
                when (job.type) {
                    nextTickType -> nextTick()
                    seedType -> JsonObject(emptyMap()).also { seed(skipActive = true) }
                    else -> JsonObject(emptyMap()).also { awaitBatch() }
                }
            }
            val watcher = launch {
                stopping.first { it }
                work.cancel()
            }
            val variables = try {
                work.await()
            } catch (e: CancellationException) {
                if (!isActive || !stopping.value) throw e
                withContext(NonCancellable) { attempt { driver.release(job) } }
                return@supervisorScope
            } catch (e: Throwable) {
                attempt { driver.fail(job, 0, Duration.ZERO, e.message ?: e.toString(), e.stackTraceToString()) }
                abort(e)
                return@supervisorScope
            } finally {
                watcher.cancel()
            }
            deliver { driver.complete(job, variables) }
        } finally {
            heartbeat.cancel()
        }
    }

    private fun nextTick(): JsonObject {
        val cron = config.start as Start.Cron
        val tick = CronSchedule.next(cron.expr, Clock.System.now(), cron.zone)
        return JsonObject(mapOf(EngineNames.NEXT_TICK to JsonPrimitive(tick.toString().removeSuffix("Z") + "+00:00")))
    }

    private suspend fun startSchedulerInstance(processId: String): String = withContext(NonCancellable) {
        val variables = JsonObject(mapOf(EngineNames.WORKFLOW to JsonPrimitive(run.workflow.key)))
        driver.startInstance(processId, variables, null).also { id ->
            scheduler = id
            if (halting) attempt { driver.cancel(id) }
        }
    }

    private suspend fun awaitBatch() {
        while (pendingRoots.isNotEmpty()) delay(pollInterval)
        run.clearInstanceState()
    }

    private suspend fun adopt(): Boolean {
        awaitRunning()
        val roots = attempt { driver.activeRoots(rootProcessId) } ?: return true
        val fresh = roots.count { root ->
            val added = root.id !in finishedRoots && pendingRoots.putIfAbsent(root.id, Root(root.itemKey, false)) == null
            if (added) root.itemKey?.let { rootsByKey[it] = root.id }
            added
        }
        run.recordAttached(fresh)
        return pendingRoots.isNotEmpty()
    }

    private suspend fun startRequeued(records: List<DeadLetterRecord>) {
        val starts = Semaphore(config.maxConcurrency)
        coroutineScope {
            for (record in records) {
                awaitRunning()
                inFlight?.acquire()
                starts.acquire()
                launch {
                    try {
                        val rootKey = record.deadLetter.key.takeIf { record.itemVariable == null }
                        if (startRoot(rootKey) { driver.startInstance(record.level, run.requeueVariables(record), rootKey) }) {
                            run.recordAttached(1)
                            config.deadLetters?.remove(record.id)
                        }
                    } finally {
                        starts.release()
                    }
                }
            }
        }
    }

    private suspend fun seed(skipActive: Boolean = false) {
        val seen = if (config.dedupe) HashSet<String>() else null
        val starts = Semaphore(config.maxConcurrency)
        coroutineScope {
            run.workflow.seed().collect { item ->
                awaitRunning()
                if (skipActive && item.key?.let { rootsByKey[it] }?.let(pendingRoots::containsKey) == true) {
                    run.recordSkipped()
                    return@collect
                }
                inFlight?.acquire()
                if (!run.admit(item, seen)) {
                    inFlight?.release()
                    return@collect
                }
                starts.acquire()
                launch {
                    try {
                        startRoot(item.key) { driver.startInstance(rootProcessId, run.rootVariables(item), item.key) }
                    } finally {
                        starts.release()
                    }
                }
            }
        }
    }

    private suspend fun startRoot(key: String?, start: suspend () -> String): Boolean = withContext(NonCancellable) {
        val id = try {
            start()
        } catch (e: Throwable) {
            inFlight?.release()
            throw e
        }
        pendingRoots[id] = Root(key, inFlight != null)
        key?.let { rootsByKey[it] = id }
        if (halting) cancelRoot(id)
        !halting
    }

    private fun releaseInFlight(root: Root) {
        if (root.holdsPermit) inFlight?.release()
    }

    private suspend fun drain(starting: Job?) {
        while (true) {
            deliverMessages()
            val started = starting?.isCompleted ?: true
            if (scheduleProcessId == null && started && pendingRoots.isEmpty() && (!attach || !adopt())) return
            delay(pollInterval)
            val finished = attempt { driver.finished(pendingRoots.keys.toList()) } ?: continue
            for ((id, completed) in finished) {
                val root = pendingRoots.remove(id) ?: continue
                finishedRoots += id
                releaseInFlight(root)
                val state = run.rootFinished(id, root.key, completed)
                root.key?.let {
                    rootsByKey.remove(it, id)
                    finishedItems.record(it, state)
                }
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
            val accepted = jobs.take(slots).filter { executing.add(it.id) }
            repeat(slots - accepted.size) { workSlots.release() }
            for (job in accepted) {
                scope.launch {
                    try {
                        process(job, fetchedAt)
                    } finally {
                        executing.remove(job.id)
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

    private fun heartbeat(job: EngineJob, lockedAt: TimeSource.Monotonic.ValueTimeMark): Job = scope.launch {
        val interval = config.lockDuration / 3
        var tick = 1
        while (true) {
            delay(interval * tick - lockedAt.elapsedNow())
            tick++
            launch {
                val extended = attempt { driver.extendLock(job, config.lockDuration) } != null
                run.recordLockExtension(job, extended)
            }
        }
    }

    private suspend fun process(job: EngineJob, lockedAt: TimeSource.Monotonic.ValueTimeMark) {
        val heartbeat = heartbeat(job, lockedAt)
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
        halting = true
        withContext(NonCancellable) { cancelRoots() }
        finishExceptionally(cause)
        scope.cancel()
    }

    private suspend fun cancelRoots() {
        scheduler?.let { attempt { driver.cancel(it) } }
        for (id in pendingRoots.keys.toList()) cancelRoot(id)
    }

    private suspend fun cancelRoot(id: String) {
        attempt { driver.cancel(id) }
        pendingRoots.remove(id)
    }

    private suspend fun deliverMessages() {
        for (pending in pendingSends.toList()) {
            if (correlate(pending.first, pending.second)) pendingSends.remove(pending)
        }
        for (message in broadcasts.toList()) correlate(message, null)
    }

    private suspend fun correlate(message: String, key: String?): Boolean {
        var correlated = false
        for (workflowKey in run.correlationKeys) {
            if (attempt { driver.correlate(message, workflowKey, key) } == true) correlated = true
        }
        return correlated
    }

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
