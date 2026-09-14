package loadshift.core

import kotlinx.serialization.json.JsonObject

class Workflow<W : WorkItem>(
    val key: String,
    val name: String,
    val seed: Seed<W>,
    val root: SubFlow<W>,
    val version: String? = null,
)

interface Backend {
    suspend fun <W : WorkItem> run(workflow: Workflow<W>, config: RunConfig = RunConfig()): RunHandle

    suspend fun <W : WorkItem> requeue(
        workflow: Workflow<W>,
        records: List<DeadLetterRecord>,
        config: RunConfig = RunConfig(),
    ): RunHandle

    suspend fun signal(name: String)

    suspend fun userTasks(workflow: Workflow<*>): List<UserTask>

    suspend fun completeUserTask(taskId: String, form: JsonObject): Boolean
}

data class UserTask(
    val id: String,
    val workflowKey: String,
    val name: String,
    val itemKey: String?,
    val assignee: String?,
    val candidateGroups: List<String>,
)

class HumanTaskRef(val owner: Workflow<*>, val levelKey: String, val step: HumanTask<*>)

fun Workflow<*>.humanTasks(): List<HumanTaskRef> = (listOf(this) + calledWorkflows()).flatMap { owner ->
    owner.levels().values.flatMap { level ->
        val found = mutableListOf<HumanTaskRef>()
        fun walk(step: Step<*>) {
            when (step) {
                is Sequence<*> -> step.steps.forEach(::walk)
                is Conditional<*> -> {
                    walk(step.onTrue)
                    step.onFalse?.let(::walk)
                }
                is Loop<*> -> walk(step.body)
                is Parallel<*> -> step.branches.forEach(::walk)
                is Timeout<*> -> walk(step.body)
                is HumanTask<*> -> found += HumanTaskRef(owner, level.key, step)
                is FanOut<*, *>, is FanIn<*, *, *>, is Call<*>, is Execute<*>, is Wait<*>, is AwaitMessage<*>, is AwaitSignal<*> -> Unit
            }
        }
        walk(level.step)
        found
    }
}

class WorkflowLevel(
    val key: String,
    val step: Step<*>,
    val codec: WorkItemCodec<*>,
    val itemVariable: String?,
    val ancestorCodecs: List<WorkItemCodec<*>>,
)

fun Workflow<*>.levels(): Map<String, WorkflowLevel> {
    val levels = LinkedHashMap<String, WorkflowLevel>()
    fun visit(sub: SubFlow<*>, itemVariable: String?, ancestors: List<WorkItemCodec<*>>) {
        levels[sub.key] = WorkflowLevel(sub.key, sub.step, sub.codec, itemVariable, ancestors)
        fun walk(step: Step<*>) {
            when (step) {
                is Sequence<*> -> step.steps.forEach(::walk)
                is Conditional<*> -> {
                    walk(step.onTrue)
                    step.onFalse?.let(::walk)
                }
                is Loop<*> -> walk(step.body)
                is Parallel<*> -> step.branches.forEach(::walk)
                is Timeout<*> -> walk(step.body)
                is FanOut<*, *> -> visit(step.body, EngineNames.item(step.id), listOf(sub.codec) + ancestors)
                is FanIn<*, *, *> -> visit(step.body, EngineNames.item(step.id), listOf(sub.codec) + ancestors)
                is Execute<*>, is Wait<*>, is AwaitMessage<*>, is AwaitSignal<*>, is Call<*>, is HumanTask<*> -> Unit
            }
        }
        walk(sub.step)
    }
    visit(root, null, emptyList())
    return levels
}

fun Workflow<*>.calledWorkflows(): List<Workflow<*>> {
    val found = LinkedHashMap<String, Workflow<*>>()
    fun walk(step: Step<*>) {
        when (step) {
            is Sequence<*> -> step.steps.forEach(::walk)
            is Conditional<*> -> {
                walk(step.onTrue)
                step.onFalse?.let(::walk)
            }
            is Loop<*> -> walk(step.body)
            is Parallel<*> -> step.branches.forEach(::walk)
            is Timeout<*> -> walk(step.body)
            is FanOut<*, *> -> walk(step.body.step)
            is FanIn<*, *, *> -> walk(step.body.step)
            is Call<*> -> {
                val target = step.workflow
                require(target.key != key) { "workflow '$key' calls itself" }
                val known = found.putIfAbsent(target.key, target)
                require(known == null || known === target) { "two different workflows with the key '${target.key}' are called" }
                if (known == null) walk(target.root.step)
            }
            is Execute<*>, is Wait<*>, is AwaitMessage<*>, is AwaitSignal<*>, is HumanTask<*> -> Unit
        }
    }
    walk(root.step)
    return found.values.toList()
}

fun Workflow<*>.requireRequeueable(records: List<DeadLetterRecord>, config: RunConfig): Map<String, WorkflowLevel> {
    require(config.start !is Start.Cron) { "a requeue run cannot use Start.Cron" }
    val duplicate = records.groupBy { it.id }.entries.firstOrNull { it.value.size > 1 }?.key
    require(duplicate == null) { "dead letter $duplicate is listed more than once" }
    val levels = levels()
    for (record in records) {
        require(record.workflowKey == key) {
            "dead letter ${record.id} belongs to workflow '${record.workflowKey}', not '$key'"
        }
        val level = requireNotNull(levels[record.level]) {
            "dead letter ${record.id} refers to level '${record.level}', which workflow '$key' does not contain"
        }
        require(record.itemVariable == level.itemVariable) {
            "dead letter ${record.id} uses item variable '${record.itemVariable}', level '${level.key}' uses '${level.itemVariable}'"
        }
    }
    return levels
}
