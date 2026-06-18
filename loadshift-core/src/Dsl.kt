package loadshift.core

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlin.time.Duration

@DslMarker
annotation class LoadshiftDsl

class IdGen {
    private var n = 0
    fun next(prefix: String): String = "$prefix${++n}"
}

internal fun sanitizeId(s: String): String =
    s.lowercase().replace(Regex("[^a-z0-9_\\-]"), "-")

@LoadshiftDsl
open class FlowSpec<W : WorkItem> internal constructor(
    @PublishedApi internal val codec: WorkItemCodec<W>,
    @PublishedApi internal val baseKey: String,
    @PublishedApi internal val idgen: IdGen,
) {
    internal val steps = mutableListOf<Step<W>>()
    internal val tasks = mutableMapOf<String, Task<W>>()
    internal val decisions = mutableMapOf<String, (W) -> Boolean>()

    fun task(
        t: Task<W>,
        retry: RetryPolicy? = null,
        timeout: Duration? = null,
        rateLimit: Rate? = null,
    ) {
        tasks[t.topic] = t
        steps += Execute(t, TaskOptions(retry, timeout, rateLimit))
    }

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) = body(item)
        }
        task(t, retry, timeout, rateLimit)
    }

    fun task(
        topic: Topic<W>,
        retry: RetryPolicy? = null,
        timeout: Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (W) -> Unit,
    ) = task(topic.name, retry, timeout, rateLimit, body)

    fun condition(predicate: (W) -> Boolean, then: FlowSpec<W>.() -> Unit): ElseHandle<W> {
        val id = idgen.next("c")
        decisions["decision_$id"] = predicate
        val thenSpec = child().apply(then)
        absorb(thenSpec)
        val index = steps.size
        steps += Conditional(id, predicate, thenSpec.toStep(), null)
        return ElseHandle(this, index, id, predicate, thenSpec.toStep())
    }

    fun loop(predicate: (W) -> Boolean, body: FlowSpec<W>.() -> Unit) {
        val id = idgen.next("l")
        decisions["decision_$id"] = predicate
        val bodySpec = child().apply(body)
        absorb(bodySpec)
        steps += Loop(id, predicate, bodySpec.toStep())
    }

    fun parallel(build: ParallelSpec<W>.() -> Unit) {
        val ps = ParallelSpec(this).apply(build)
        steps += Parallel(ps.branchSteps.toList())
    }

    inline fun <reified C : WorkItem> fanOut(
        noinline expand: suspend (W) -> List<C>,
        concurrency: Int? = null,
        noinline body: FlowSpec<C>.() -> Unit,
    ) = fanOutInternal(
        workItemCodec<C>(),
        { w -> kotlinx.coroutines.flow.flow { for (c in expand(w)) emit(c) } },
        concurrency,
        body,
    )

    inline fun <reified C : WorkItem> fanOut(
        paginated: Paginated<W, C>,
        concurrency: Int? = null,
        noinline body: FlowSpec<C>.() -> Unit,
    ) = fanOutInternal(workItemCodec<C>(), { w -> paginated.asFlow(w) }, concurrency, body)

    @PublishedApi
    internal fun <C : WorkItem> fanOutInternal(
        childCodec: WorkItemCodec<C>,
        expand: suspend (W) -> Flow<C>,
        concurrency: Int?,
        body: FlowSpec<C>.() -> Unit,
    ) {
        val id = idgen.next("f")
        val childKey = "${baseKey}_$id"
        val childSpec = FlowSpec(childCodec, childKey, IdGen()).apply(body)
        val sub = SubFlow(
            childKey,
            childSpec.toStep(),
            childCodec,
            childSpec.tasks.toMap(),
            childSpec.decisions.toMap(),
        )
        steps += FanOut(id, childKey, expand, childCodec, concurrency, sub)
    }

    internal fun setElse(
        index: Int,
        id: String,
        predicate: (W) -> Boolean,
        thenStep: Step<W>,
        block: FlowSpec<W>.() -> Unit,
    ) {
        val elseSpec = child().apply(block)
        absorb(elseSpec)
        steps[index] = Conditional(id, predicate, thenStep, elseSpec.toStep())
    }

    internal fun child(): FlowSpec<W> = FlowSpec(codec, baseKey, idgen)

    internal fun absorb(other: FlowSpec<W>) {
        tasks.putAll(other.tasks)
        decisions.putAll(other.decisions)
    }

    internal fun toStep(): Step<W> = Sequence(steps.toList())
}

class ElseHandle<W : WorkItem> internal constructor(
    private val parent: FlowSpec<W>,
    private val index: Int,
    private val id: String,
    private val predicate: (W) -> Boolean,
    private val thenStep: Step<W>,
) {
    infix fun otherwise(block: FlowSpec<W>.() -> Unit) {
        parent.setElse(index, id, predicate, thenStep, block)
    }
}

@LoadshiftDsl
class ParallelSpec<W : WorkItem> internal constructor(private val parent: FlowSpec<W>) {
    internal val branchSteps = mutableListOf<Step<W>>()

    fun branch(body: FlowSpec<W>.() -> Unit) {
        val spec = parent.child().apply(body)
        parent.absorb(spec)
        branchSteps += spec.toStep()
    }
}

@LoadshiftDsl
class WorkflowSpec<W : WorkItem> @PublishedApi internal constructor(
    codec: WorkItemCodec<W>,
    private val wfName: String,
) : FlowSpec<W>(codec, sanitizeId(wfName), IdGen()) {
    private var seed: Seed<W> = { emptyFlow() }

    fun input(list: List<W>) {
        seed = { flow { for (w in list) emit(w) } }
    }

    fun input(vararg items: W) = input(items.toList())

    fun input(seed: suspend () -> Flow<W>) {
        this.seed = seed
    }

    fun input(paginated: Paginated<Unit, W>) {
        seed = { paginated.asFlow(Unit) }
    }

    @PublishedApi
    internal fun build(): Workflow<W> {
        val key = sanitizeId(wfName)
        return Workflow(
            key = key,
            name = wfName,
            seed = seed,
            root = SubFlow(key, toStep(), codec, tasks.toMap(), decisions.toMap()),
        )
    }
}

inline fun <reified W : WorkItem> workflow(
    name: String,
    build: WorkflowSpec<W>.() -> Unit,
): Workflow<W> = WorkflowSpec<W>(workItemCodec<W>(), name).apply(build).build()
