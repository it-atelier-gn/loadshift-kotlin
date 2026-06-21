package loadshift.core

import kotlinx.coroutines.flow.Flow

sealed interface Step<W : WorkItem>

class Sequence<W : WorkItem>(val steps: List<Step<W>>) : Step<W>

class Execute<W : WorkItem>(val task: Task<W>, val options: TaskOptions) : Step<W>

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
