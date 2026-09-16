package loadshift.core

import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.json.JsonObject
import kotlin.reflect.KClass
import kotlin.time.Duration

sealed interface Step<W : WorkItem>

class Sequence<W : WorkItem>(val steps: List<Step<W>>) : Step<W>

class Execute<W : WorkItem>(
    val task: Task<W>,
    val options: TaskOptions,
    var compensation: (suspend (W) -> Unit)? = null,
    val catches: MutableList<Catch<W>> = mutableListOf(),
) : Step<W>

class Catch<W : WorkItem>(val id: String, val type: KClass<out Throwable>, val body: Step<W>)

class Conditional<W : WorkItem>(
    val id: String,
    val predicate: (W) -> Boolean,
    val onTrue: Step<W>,
    val onFalse: Step<W>?,
) : Step<W>

class Loop<W : WorkItem>(
    val id: String,
    val predicate: (W) -> Boolean,
    val body: Step<W>,
) : Step<W>

class Parallel<W : WorkItem>(val branches: List<Step<W>>) : Step<W>

class Wait<W : WorkItem>(val id: String, val duration: Duration) : Step<W>

class Timeout<W : WorkItem>(val id: String, val duration: Duration, val body: Step<W>) : Step<W>

class AwaitMessage<W : WorkItem>(
    val id: String,
    val message: String,
    val timeout: Duration? = null,
    val onMessage: (suspend (W, JsonObject) -> Unit)? = null,
    var onTimeout: Step<W>? = null,
) : Step<W>

class AwaitSignal<W : WorkItem>(val id: String, val signal: String) : Step<W>

class Call<W : WorkItem>(val id: String, val workflow: Workflow<W>) : Step<W>

class HumanTask<W : WorkItem>(
    val id: String,
    val name: String,
    val assignee: String?,
    val candidateGroups: List<String>,
    val onComplete: suspend (W, JsonObject) -> Unit,
) : Step<W>

class FanOut<W : WorkItem, C : WorkItem>(
    val id: String,
    val childKey: String,
    val expand: suspend (W) -> Flow<C>,
    val childCodec: WorkItemCodec<C>,
    val concurrency: Int?,
    val body: SubFlow<C>,
) : Step<W>

class FanIn<W : WorkItem, C : WorkItem, A>(
    val id: String,
    val childKey: String,
    val expand: suspend (W) -> Flow<C>,
    val childCodec: WorkItemCodec<C>,
    val concurrency: Int?,
    val body: SubFlow<C>,
    val initial: A,
    val combine: (A, C) -> A,
    val onComplete: suspend (W, A) -> Unit,
) : Step<W>

class SubFlow<W : WorkItem>(
    val key: String,
    val step: Step<W>,
    val codec: WorkItemCodec<W>,
    val tasks: Map<String, Task<W>>,
    val decisions: Map<String, (W) -> Boolean>,
)
