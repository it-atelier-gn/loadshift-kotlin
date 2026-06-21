package loadshift.core

import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

enum class RunState { Scheduled, Running, Paused, Completed, Failed, Cancelled }

private val TERMINAL_STATES = setOf(RunState.Completed, RunState.Failed, RunState.Cancelled)

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
)

interface RunInspector {
    fun state(): RunState
    fun progress(): Progress
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
    suspend fun cancel(id: String): Boolean
}

interface ControllableBackend : Backend {
    val control: Control
}

class RunTracker(override val backendType: String, private val maxEntries: Int = 1000) : Control {

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

    fun track(workflow: Workflow<*>, inspector: RunInspector, control: RunHandle? = null): String {
        val id = "run-${counter.incrementAndGet()}"
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
        return id
    }

    private fun evictOldestCompleted() {
        while (entries.size > maxEntries) {
            val oldest = entries.values
                .filter { it.inspector.state() in TERMINAL_STATES }
                .minByOrNull { it.startedAt }
                ?: break
            entries.remove(oldest.id)
        }
    }

    override suspend fun runs(): List<RunSnapshot> =
        entries.values.sortedBy { it.startedAt }.map { snapshot(it) }

    override suspend fun run(id: String): RunSnapshot? = entries[id]?.let { snapshot(it) }

    override fun structure(id: String): FlowNode? = entries[id]?.structure

    override suspend fun start(id: String): Boolean = withControl(id) { it.start() }

    override suspend fun pause(id: String): Boolean = withControl(id) { it.pause() }

    override suspend fun cancel(id: String): Boolean = withControl(id) { it.cancel() }

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
    )
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
    is FanOut<*, *> -> FlowNode("fanOut", step.id, listOf(describeStep(step.body.step)))
    is FanIn<*, *, *> -> FlowNode("fanIn", step.id, listOf(describeStep(step.body.step)))
}
