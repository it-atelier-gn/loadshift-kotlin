package loadshift.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

enum class RunState { Scheduled, Running, Paused, Completed, Failed, Cancelled, Detached }

private val TERMINAL_STATES = setOf(RunState.Completed, RunState.Failed, RunState.Cancelled, RunState.Detached)

val RunState.terminal: Boolean get() = this in TERMINAL_STATES

data class FlowNode(
    val type: String,
    val label: String,
    val children: List<FlowNode> = emptyList(),
)

data class RunSnapshot(
    val id: String,
    val workflowKey: String,
    val workflowName: String,
    val state: RunState,
    val startedAt: Instant,
    val progress: Progress,
    val deadLetters: List<DeadLetter>,
    val engineActive: Long?,
    val runId: String? = null,
)

interface RunInspector {
    fun state(): RunState
    fun progress(): Progress
    fun runId(): String? = null
    fun deadLetters(): List<DeadLetter> = emptyList()
    suspend fun engineActive(): Long? = null
}

interface Control {
    val backendType: String
    suspend fun runs(): List<RunSnapshot>
    suspend fun run(id: String): RunSnapshot?
    fun structure(id: String): FlowNode?
    suspend fun start(id: String): Boolean
    suspend fun pause(id: String): Boolean
    suspend fun resume(id: String): Boolean
    suspend fun cancel(id: String): Boolean
    suspend fun detach(id: String): Boolean
    suspend fun item(id: String, key: String): ItemStatus?
    suspend fun cancelItem(id: String, key: String): Boolean
    val worker: String? get() = null
    suspend fun registeredRuns(): List<RunRecord> = emptyList()
}

interface ControllableBackend : Backend {
    val control: Control
}

interface EngineBackend : ControllableBackend {
    suspend fun <W : WorkItem> attach(workflow: Workflow<W>, config: RunConfig = RunConfig()): RunHandle
    suspend fun migrate(workflow: Workflow<*>): MigrationResult
}

class RunTracker(
    override val backendType: String,
    private val maxEntries: Int = 1000,
    private val registry: RunRegistry? = null,
    override val worker: String = defaultWorkerName(),
    private val publishInterval: Duration = 5.seconds,
) : Control {

    init {
        require(publishInterval.isPositive()) { "publishInterval must be positive, was $publishInterval" }
    }

    private class Entry(
        val id: String,
        val workflowKey: String,
        val workflowName: String,
        val startedAt: Instant,
        val structure: FlowNode,
        val inspector: RunInspector,
        val control: RunHandle?,
    )

    private val counter = AtomicLong()
    private val entries = ConcurrentHashMap<String, Entry>()
    private val publisher = registry?.let { CoroutineScope(SupervisorJob() + Dispatchers.IO) }
    private val publishing = AtomicBoolean(false)
    private val finalPublished: MutableSet<String> = ConcurrentHashMap.newKeySet()

    fun track(workflow: Workflow<*>, inspector: RunInspector, control: RunHandle? = null): String {
        val number = counter.incrementAndGet()
        val id = if (registry == null) "run-$number" else "$worker:run-$number"
        entries[id] = Entry(
            id = id,
            workflowKey = workflow.key,
            workflowName = workflow.name,
            startedAt = Clock.System.now(),
            structure = describeFlow(workflow),
            inspector = inspector,
            control = control,
        )
        evictOldestCompleted()
        startPublishing()
        return id
    }

    suspend fun publish() {
        val target = registry ?: return
        for (entry in entries.values) {
            if (entry.id in finalPublished) continue
            val now = Clock.System.now()
            val state = entry.inspector.state()
            val record = RunRecord(
                id = entry.id,
                worker = worker,
                backendType = backendType,
                workflowKey = entry.workflowKey,
                workflowName = entry.workflowName,
                state = state,
                startedAt = entry.startedAt,
                updatedAt = now,
                staleAt = now + publishInterval * STALE_INTERVALS,
                progress = entry.inspector.progress(),
                deadLetters = entry.inspector.deadLetters().size.toLong(),
            )
            try {
                target.save(record)
                if (state.terminal) finalPublished += entry.id
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                continue
            }
        }
    }

    override suspend fun registeredRuns(): List<RunRecord> {
        val target = registry ?: return emptyList()
        val records = try {
            target.list()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            return emptyList()
        }
        return records.filter { !entries.containsKey(it.id) }
    }

    private fun startPublishing() {
        val scope = publisher ?: return
        if (!publishing.compareAndSet(false, true)) return
        scope.launch {
            while (true) {
                publish()
                delay(publishInterval)
            }
        }
    }

    private fun evictOldestCompleted() {
        while (entries.size > maxEntries) {
            val oldest = entries.values
                .filter { it.inspector.state().terminal }
                .minByOrNull { it.startedAt }
                ?: break
            entries.remove(oldest.id)
            finalPublished.remove(oldest.id)
        }
    }

    override suspend fun runs(): List<RunSnapshot> =
        entries.values.sortedBy { it.startedAt }.map { snapshot(it) }

    override suspend fun run(id: String): RunSnapshot? = entries[id]?.let { snapshot(it) }

    override fun structure(id: String): FlowNode? = entries[id]?.structure

    override suspend fun start(id: String): Boolean = withControl(id) { it.start() }

    override suspend fun pause(id: String): Boolean = withControl(id) { it.pause() }

    override suspend fun resume(id: String): Boolean = withControl(id) { it.resume() }

    override suspend fun cancel(id: String): Boolean = withControl(id) { it.cancel() }

    override suspend fun detach(id: String): Boolean = withControl(id) { it.detach() }

    override suspend fun item(id: String, key: String): ItemStatus? = entries[id]?.control?.item(key)

    override suspend fun cancelItem(id: String, key: String): Boolean = entries[id]?.control?.cancelItem(key) ?: false

    private suspend fun withControl(id: String, action: suspend (RunHandle) -> Unit): Boolean {
        val control = entries[id]?.control ?: return false
        action(control)
        return true
    }

    private suspend fun snapshot(e: Entry): RunSnapshot = RunSnapshot(
        id = e.id,
        workflowKey = e.workflowKey,
        workflowName = e.workflowName,
        state = e.inspector.state(),
        startedAt = e.startedAt,
        progress = e.inspector.progress(),
        deadLetters = e.inspector.deadLetters(),
        engineActive = e.inspector.engineActive(),
        runId = e.inspector.runId(),
    )

    private companion object {
        const val STALE_INTERVALS = 3
    }
}

fun describeFlow(workflow: Workflow<*>): FlowNode =
    FlowNode("workflow", workflow.name, listOf(describeStep(workflow.root.step)))

private fun describeStep(step: Step<*>): FlowNode = when (step) {
    is Sequence<*> -> FlowNode("sequence", "", step.steps.map { describeStep(it) })
    is Execute<*> -> FlowNode("task", step.task.topic)
    is Conditional<*> -> FlowNode(
        "if",
        step.id,
        listOfNotNull(
            FlowNode("then", "", listOf(describeStep(step.onTrue))),
            step.onFalse?.let { FlowNode("else", "", listOf(describeStep(it))) },
        ),
    )
    is Loop<*> -> FlowNode("loop", step.id, listOf(describeStep(step.body)))
    is Parallel<*> -> FlowNode("parallel", "", step.branches.map { describeStep(it) })
    is Wait<*> -> FlowNode("wait", step.id)
    is Timeout<*> -> FlowNode("timeout", step.id, listOf(describeStep(step.body)))
    is AwaitMessage<*> -> FlowNode("awaitMessage", step.message)
    is AwaitSignal<*> -> FlowNode("awaitSignal", step.signal)
    is Call<*> -> FlowNode("call", step.workflow.name)
    is HumanTask<*> -> FlowNode("userTask", step.name)
    is FanOut<*, *> -> FlowNode("fanOut", step.id, listOf(describeStep(step.body.step)))
    is FanIn<*, *, *> -> FlowNode("fanIn", step.id, listOf(describeStep(step.body.step)))
}
