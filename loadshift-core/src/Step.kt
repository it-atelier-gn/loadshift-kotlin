package loadshift.core

import kotlinx.coroutines.flow.Flow

sealed interface Step<W : WorkItemBase>

class Sequence<W : WorkItemBase>(val steps: List<Step<W>>) : Step<W>

class Execute<W : WorkItemBase>(val task: Task<W>, val options: TaskOptions) : Step<W>

class Conditional<W : WorkItemBase>(
    val id: String,
    val predicate: (W) -> Boolean,
    val onTrue: Step<W>,
    val onFalse: Step<W>?,
) : Step<W>

class Loop<W : WorkItemBase>(
    val id: String,
    val predicate: (W) -> Boolean,
    val body: Step<W>,
) : Step<W>

class Parallel<W : WorkItemBase>(val branches: List<Step<W>>) : Step<W>

class FanOut<W : WorkItemBase, C : WorkItemBase>(
    val id: String,
    val childKey: String,
    val expand: suspend (W) -> Flow<C>,
    val childFactory: (MutableMap<String, Any?>) -> C,
    val concurrency: Int?,
    val body: SubFlow<C>,
) : Step<W>

class SubFlow<W : WorkItemBase>(
    val key: String,
    val step: Step<W>,
    val factory: (MutableMap<String, Any?>) -> W,
    val tasks: Map<String, Task<W>>,
    val decisions: Map<String, (W) -> Boolean>,
)
