package loadshift.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Serializable
private data class RunnerItem(var id: String) : WorkItem {
    override val key get() = id
}

@OptIn(EngineApi::class)
private class FakeDriver : EngineDriver {
    class Failure(val jobId: String, val retries: Int, val backoff: Duration)

    val started = CopyOnWriteArrayList<Pair<String, JsonObject>>()
    val jobs = Channel<EngineJob>(Channel.UNLIMITED)
    val completed = CopyOnWriteArrayList<String>()
    val failures = CopyOnWriteArrayList<Failure>()
    val terminated = CopyOnWriteArrayList<String>()
    val cancelled = CopyOnWriteArrayList<String>()
    val finished = ConcurrentHashMap<String, Boolean>()
    val waiting: MutableSet<Pair<String, String?>> = ConcurrentHashMap.newKeySet()
    val correlationAttempts = AtomicInteger()
    val lockExtensions = AtomicInteger()
    val released = CopyOnWriteArrayList<String>()

    @Volatile
    var heldFetch: CompletableDeferred<List<EngineJob>>? = null
    private val ids = AtomicInteger()

    val roots = CopyOnWriteArrayList<RootInstance>()

    override fun pollGroups(jobTypes: List<String>): List<List<String>> = listOf(jobTypes)

    override suspend fun activeRoots(processId: String): List<RootInstance> = roots.filter { finished[it.id] == null }

    override suspend fun startInstance(processId: String, variables: JsonObject, businessKey: String?): String {
        val id = "pi-${ids.incrementAndGet()}"
        started += id to variables
        return id
    }

    override suspend fun fetch(jobTypes: List<String>, maxJobs: Int, lock: Duration, wait: Duration): List<EngineJob> {
        heldFetch?.let { held ->
            heldFetch = null
            return held.await()
        }
        val first = withTimeoutOrNull(wait.coerceAtLeast(10.milliseconds)) { jobs.receive() } ?: return emptyList()
        val batch = mutableListOf(first)
        while (batch.size < maxJobs) batch += jobs.tryReceive().getOrNull() ?: break
        return batch
    }

    override suspend fun complete(job: EngineJob, variables: JsonObject) {
        completed += job.id
    }

    override suspend fun fail(job: EngineJob, retries: Int, backoff: Duration, message: String, details: String) {
        failures += Failure(job.id, retries, backoff)
    }

    override suspend fun terminate(job: EngineJob, message: String, variables: JsonObject) {
        terminated += job.id
    }

    override suspend fun extendLock(job: EngineJob, lock: Duration) {
        lockExtensions.incrementAndGet()
    }

    override suspend fun release(job: EngineJob) {
        released += job.id
    }

    override suspend fun finished(instanceIds: List<String>): Map<String, Boolean> =
        instanceIds.mapNotNull { id -> finished[id]?.let { id to it } }.toMap()

    override suspend fun cancel(instanceId: String) {
        cancelled += instanceId
        finished[instanceId] = false
    }

    override suspend fun correlate(message: String, workflowKey: String, itemKey: String?): Boolean {
        correlationAttempts.incrementAndGet()
        return waiting.remove(message to itemKey)
    }

    override suspend fun activeInstances(processIds: List<String>): Long = (started.size - finished.size).toLong()

    fun enqueueWork(workflow: Workflow<*>) {
        for ((id, variables) in started) {
            jobs.trySend(EngineJob("job-$id", EngineNames.jobType(workflow.key, "work"), id, variables, null))
        }
    }

    fun finishAll() {
        for ((id, _) in started) finished[id] = true
    }
}

@OptIn(EngineApi::class)
class EngineRunnerTest {

    private fun flow(name: String, vararg ids: String, body: suspend (RunnerItem) -> Unit = {}) =
        workflow<RunnerItem>(name) {
            input(ids.map { RunnerItem(it) })
            task("work") { body(it) }
        }

    private fun launch(
        workflow: Workflow<*>,
        config: RunConfig = RunConfig(),
        driver: FakeDriver = FakeDriver(),
        attach: Boolean = false,
    ): Pair<EngineRunner, FakeDriver> {
        val runner = EngineRunner(
            run = EngineRun(workflow, config),
            driver = driver,
            rootProcessId = workflow.root.key,
            processIds = listOf(workflow.root.key),
            attach = attach,
            pollInterval = 20.milliseconds,
            fetchWait = 20.milliseconds,
        ).begin()
        return runner to driver
    }

    private suspend fun eventually(condition: () -> Boolean) {
        withTimeout(5.seconds) { while (!condition()) delay(10.milliseconds) }
    }

    @Test
    fun runCompletesOnceEveryRootInstanceFinished() = runBlocking {
        val store = InMemoryCheckpointStore()
        val wf = flow("finish", "a", "b")
        val (handle, driver) = launch(wf, RunConfig(checkpoints = store))
        eventually { driver.started.size == 2 }
        driver.enqueueWork(wf)
        eventually { driver.completed.size == 2 }
        assertEquals(RunState.Running, handle.state())

        driver.finishAll()

        assertEquals(RunResult(done = 2, failed = 0, skipped = 0, deadLetters = emptyList()), handle.await())
        assertEquals(RunState.Completed, handle.state())
        assertTrue(store.isComplete("finish", "a"))
        assertTrue(store.isComplete("finish", "b"))
    }

    @Test
    fun seedingSkipsDuplicatesAndCheckpointedItems() = runBlocking {
        val store = InMemoryCheckpointStore().apply { markComplete("seed", "done") }
        val (handle, driver) = launch(flow("seed", "a", "a", "done", "b"), RunConfig(dedupe = true, checkpoints = store))
        eventually { driver.started.size == 2 }
        delay(100.milliseconds)
        assertEquals(Progress(seeded = 2, skipped = 2), handle.progress())
        handle.cancel()
    }

    @Test
    fun pausedRunDoesNotSeedUntilResumed() = runBlocking {
        val (handle, driver) = launch(flow("paused", "a", "b"), RunConfig(start = Start.Manual))
        assertEquals(RunState.Scheduled, handle.state())
        handle.pause()
        handle.start()
        eventually { handle.state() == RunState.Paused }
        delay(150.milliseconds)
        assertTrue(driver.started.isEmpty())

        handle.resume()

        eventually { driver.started.size == 2 }
        assertEquals(RunState.Running, handle.state())
        handle.cancel()
    }

    @Test
    fun pausedRunFetchesNoJobs() = runBlocking {
        val wf = flow("fetch", "a")
        val (handle, driver) = launch(wf)
        eventually { driver.started.size == 1 }
        handle.pause()
        delay(100.milliseconds)
        driver.enqueueWork(wf)
        delay(200.milliseconds)
        assertTrue(driver.completed.isEmpty())

        handle.resume()

        eventually { driver.completed.size == 1 }
        handle.cancel()
    }

    @Test
    fun failedJobsReportRemainingRetriesAndBackoff() = runBlocking {
        val wf = flow("retry", "a") { error("boom") }
        val retry = RetryPolicy(maxAttempts = 3, baseDelay = 100.milliseconds, jitter = false)
        val (handle, driver) = launch(wf, RunConfig(retry = retry))
        eventually { driver.started.size == 1 }
        driver.enqueueWork(wf)
        eventually { driver.failures.size == 1 }
        val failure = driver.failures.single()
        assertEquals(2, failure.retries)
        assertEquals(100.milliseconds, failure.backoff)
        handle.cancel()
    }

    @Test
    fun deadLetteredItemsEndThroughTheEngineAndAreNotCountedDone() = runBlocking {
        val wf = flow("terminate", "a") { error("rejected") }
        val (handle, driver) = launch(wf, RunConfig(retry = RetryPolicy.None))
        eventually { driver.started.size == 1 }
        driver.enqueueWork(wf)
        eventually { driver.terminated.size == 1 }

        driver.finishAll()

        val result = handle.await()
        assertEquals(0, result.done)
        assertEquals(listOf(DeadLetter("a", "work", "rejected")), result.deadLetters)
    }

    @Test
    fun failPolicyCancelsInstancesAndFailsTheRun() = runBlocking {
        val wf = flow("abort", "a", "b") { if (it.id == "a") error("fatal") }
        val (handle, driver) = launch(wf, RunConfig(onError = ErrorPolicy.Fail, retry = RetryPolicy.None))
        eventually { driver.started.size == 2 }
        driver.enqueueWork(wf)

        val failure = runCatching { handle.await() }.exceptionOrNull()

        assertEquals("fatal", failure?.message)
        assertEquals(RunState.Failed, handle.state())
        assertEquals(driver.started.map { it.first }.toSet(), driver.cancelled.toSet())
        assertTrue(driver.failures.any { it.retries == 0 })
        assertEquals(1, handle.progress().failed)
    }

    @Test
    fun cancelCancelsPendingInstances() = runBlocking {
        val (handle, driver) = launch(flow("cancel", "a", "b"))
        eventually { driver.started.size == 2 }

        handle.cancel()

        assertEquals(RunResult(done = 0, failed = 0, skipped = 0, deadLetters = emptyList()), handle.await())
        assertEquals(RunState.Cancelled, handle.state())
        assertEquals(driver.started.map { it.first }.toSet(), driver.cancelled.toSet())
    }

    @Test
    fun sendIsDeliveredOnceTheItemWaits() = runBlocking {
        val (handle, driver) = launch(flow("send", "a"))
        eventually { driver.started.size == 1 }

        handle.send("go", "a")
        val before = driver.correlationAttempts.get()
        eventually { driver.correlationAttempts.get() > before + 2 }
        driver.waiting += "go" to "a"
        eventually { driver.waiting.isEmpty() }

        val settled = driver.correlationAttempts.get()
        delay(150.milliseconds)
        assertEquals(settled, driver.correlationAttempts.get())
        handle.cancel()
    }

    @Test
    fun broadcastReachesItemsThatStartWaitingLater() = runBlocking {
        val (handle, driver) = launch(flow("broadcast", "a"))
        eventually { driver.started.size == 1 }

        handle.broadcast("open")
        delay(100.milliseconds)
        driver.waiting += "open" to null

        eventually { driver.waiting.isEmpty() }
        handle.cancel()
    }

    @Test
    fun longRunningJobsExtendTheirLock() = runBlocking {
        val wf = flow("lock", "a") { delay(400.milliseconds) }
        val (handle, driver) = launch(wf, RunConfig(lockDuration = 100.milliseconds))
        eventually { driver.started.size == 1 }
        driver.enqueueWork(wf)
        eventually { driver.completed.size == 1 }
        assertTrue(driver.lockExtensions.get() >= 3, "lock extensions: ${driver.lockExtensions.get()}")
        handle.cancel()
    }

    @Test
    fun jobsFetchedWhileStoppingAreReleasedBeforeTheRunCompletes() = runBlocking {
        val held = CompletableDeferred<List<EngineJob>>()
        val wf = flow("release", "a")
        val (handle, driver) = launch(wf, driver = FakeDriver().apply { heldFetch = held })
        eventually { driver.started.size == 1 }

        driver.finishAll()
        delay(150.milliseconds)
        assertEquals(RunState.Running, handle.state())

        held.complete(listOf(EngineJob("stray", EngineNames.jobType(wf.key, "work"), "pi-other", JsonObject(emptyMap()), null)))

        assertEquals(1, handle.await().done)
        assertEquals(listOf("stray"), driver.released.toList())
        assertTrue(driver.completed.isEmpty())
    }

    @Test
    fun attachedRunWorksExistingInstancesUntilNoneRemain() = runBlocking {
        val wf = flow("attached", "unused")
        val driver = FakeDriver().apply {
            roots += RootInstance("pi-old-1", "a")
            roots += RootInstance("pi-old-2", "b")
        }
        val (handle, _) = launch(wf, driver = driver, attach = true)
        eventually { handle.progress().seeded == 2L }

        driver.jobs.trySend(
            EngineJob("job-old-1", EngineNames.jobType(wf.key, "work"), "pi-old-1", JsonObject(mapOf("id" to JsonPrimitive("a"))), null),
        )
        eventually { driver.completed.size == 1 }
        driver.finished["pi-old-1"] = true
        driver.roots += RootInstance("pi-late", "c")
        delay(100.milliseconds)
        driver.finished["pi-old-2"] = true
        eventually { handle.progress().seeded == 3L }
        driver.finished["pi-late"] = true

        assertEquals(3, handle.await().done)
        assertTrue(driver.started.isEmpty())
    }

    @Test
    fun attachRejectsCronStarts() {
        assertFailsWith<IllegalArgumentException> {
            launch(flow("attach-cron", "a"), RunConfig(start = Start.Cron("* * * * *")), attach = true)
        }
    }

    @Test
    fun detachWaitsForRunningTasksAndLeavesInstancesAlone() = runBlocking {
        val gate = CompletableDeferred<Unit>()
        val running = CompletableDeferred<Unit>()
        val wf = flow("detach", "a") {
            running.complete(Unit)
            gate.await()
        }
        val (handle, driver) = launch(wf)
        eventually { driver.started.size == 1 }
        driver.enqueueWork(wf)
        running.await()

        val detaching = async { handle.detach() }
        delay(150.milliseconds)
        assertTrue(detaching.isActive)
        gate.complete(Unit)
        detaching.await()

        assertEquals(RunState.Detached, handle.state())
        assertEquals(listOf("job-pi-1"), driver.completed.toList())
        assertTrue(driver.cancelled.isEmpty())
        assertEquals(RunResult(done = 0, failed = 0, skipped = 0, deadLetters = emptyList()), handle.await())
    }

    @Test
    fun dryRunStartsNoInstances() = runBlocking {
        val (handle, driver) = launch(flow("dry", "a"), RunConfig(dryRun = true))
        assertEquals(RunResult(done = 0, failed = 0, skipped = 0, deadLetters = emptyList()), handle.await())
        assertEquals(RunState.Completed, handle.state())
        assertTrue(driver.started.isEmpty())
    }
}
