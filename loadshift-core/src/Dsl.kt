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
    ): TaskHandle<W> {
        tasks[t.topic] = t
        val e = Execute(t, TaskOptions(retry, timeout, rateLimit))
        steps += e
        return TaskHandle(e)
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

    fun wait(duration: Duration) {
        steps += Wait(idgen.next("w"), duration)
    }

    fun timeout(duration: Duration, build: FlowSpec<W>.() -> Unit) {
        val bodySpec = child().apply(build)
        absorb(bodySpec)
        steps += Timeout(idgen.next("to"), duration, bodySpec.toStep())
    }

    fun awaitMessage(message: String) {
        steps += AwaitMessage(idgen.next("msg"), message)
    }

    internal fun replaceStep(index: Int, step: Step<W>) {
        steps[index] = step
    }

    @PublishedApi
    internal fun <C : WorkItem> fanOutInternal(
        childCodec: WorkItemCodec<C>,
        expand: suspend (W) -> Flow<C>,
        concurrency: Int?,
        buildSpec: (WorkItemCodec<C>, String, IdGen) -> FlowSpec<C>,
    ): Fan<W, C> {
        val id = idgen.next("f")
        val childKey = "${baseKey}_$id"
        val childSpec = buildSpec(childCodec, childKey, IdGen())
        val sub = SubFlow(
            childKey,
            childSpec.toStep(),
            childCodec,
            childSpec.tasks.toMap(),
            childSpec.decisions.toMap(),
        )
        val index = steps.size
        steps += FanOut(id, childKey, expand, childCodec, concurrency, sub)
        return Fan(this, index, id, childKey, expand, childCodec, concurrency, sub)
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

fun <W : WorkItem> FlowSpec<W>.task(
    topic: String,
    retry: RetryPolicy? = null,
    timeout: Duration? = null,
    rateLimit: Rate? = null,
    body: suspend (W) -> Unit,
): TaskHandle<W> {
    val t = object : Task<W>(topic) {
        override suspend fun execute(item: W) = body(item)
    }
    return task(t, retry, timeout, rateLimit)
}

class TaskHandle<W : WorkItem> internal constructor(private val step: Execute<W>) {
    infix fun compensate(block: suspend (W) -> Unit) {
        step.compensation = block
    }
}

inline fun <W : WorkItem, reified C : WorkItem> FlowSpec<W>.fanOut(
    noinline expand: suspend (W) -> List<C>,
    concurrency: Int? = null,
    noinline body: FanFlowSpec<C, Nothing?>.() -> Unit,
) = fanOutInternal(
    workItemCodec<C>(),
    { w -> flow { for (c in expand(w)) emit(c) } },
    concurrency,
) { codec, key, idgen ->
    FanFlowSpec<C, Nothing?>(codec, key, idgen) { null }.apply(body)
}

inline fun <W : WorkItem, reified C : WorkItem, Ctx> FlowSpec<W>.fanOut(
    noinline expand: suspend (W) -> List<C>,
    noinline context: suspend (W) -> Ctx,
    concurrency: Int? = null,
    noinline body: FanFlowSpec<C, Ctx>.() -> Unit,
) = fanOutInternal(
    workItemCodec<C>(),
    { w -> flow { for (c in expand(w)) emit(c) } },
    concurrency,
) { codec, key, idgen ->
    FanFlowSpec<C, Ctx>(codec, key, idgen) { d -> @Suppress("UNCHECKED_CAST") context(ancestorStack()[d] as W) }.apply(body)
}

inline fun <W : WorkItem, Ctx, reified C : WorkItem, NewCtx> FanFlowSpec<W, Ctx>.fanOut(
    noinline expand: suspend (W) -> List<C>,
    noinline context: suspend (W, Ctx) -> NewCtx,
    concurrency: Int? = null,
    noinline body: FanFlowSpec<C, NewCtx>.() -> Unit,
) = fanOutInternal(
    workItemCodec<C>(),
    { w -> flow { for (c in expand(w)) emit(c) } },
    concurrency,
) { codec, key, idgen ->
    val parentProvider = contextProvider
    FanFlowSpec<C, NewCtx>(codec, key, idgen) { d ->
        @Suppress("UNCHECKED_CAST")
        context(ancestorStack()[d] as W, parentProvider(d + 1))
    }.apply(body)
}

inline fun <W : WorkItem, reified C : WorkItem> FlowSpec<W>.fanOut(
    paginated: Paginated<W, C>,
    concurrency: Int? = null,
    noinline body: FanFlowSpec<C, Nothing?>.() -> Unit,
) = fanOutInternal(workItemCodec<C>(), { w -> paginated.asFlow(w) }, concurrency) { codec, key, idgen ->
    FanFlowSpec<C, Nothing?>(codec, key, idgen) { null }.apply(body)
}

inline fun <W : WorkItem, reified C : WorkItem, Ctx> FlowSpec<W>.fanOut(
    paginated: Paginated<W, C>,
    noinline context: suspend (W) -> Ctx,
    concurrency: Int? = null,
    noinline body: FanFlowSpec<C, Ctx>.() -> Unit,
) = fanOutInternal(workItemCodec<C>(), { w -> paginated.asFlow(w) }, concurrency) { codec, key, idgen ->
    FanFlowSpec<C, Ctx>(codec, key, idgen) { d -> @Suppress("UNCHECKED_CAST") context(ancestorStack()[d] as W) }.apply(body)
}

inline fun <W : WorkItem, Ctx, reified C : WorkItem, NewCtx> FanFlowSpec<W, Ctx>.fanOut(
    paginated: Paginated<W, C>,
    noinline context: suspend (W, Ctx) -> NewCtx,
    concurrency: Int? = null,
    noinline body: FanFlowSpec<C, NewCtx>.() -> Unit,
) = fanOutInternal(workItemCodec<C>(), { w -> paginated.asFlow(w) }, concurrency) { codec, key, idgen ->
    val parentProvider = contextProvider
    FanFlowSpec<C, NewCtx>(codec, key, idgen) { d ->
        @Suppress("UNCHECKED_CAST")
        context(ancestorStack()[d] as W, parentProvider(d + 1))
    }.apply(body)
}

class Fan<W : WorkItem, C : WorkItem> internal constructor(
    private val owner: FlowSpec<W>,
    private val index: Int,
    private val id: String,
    private val childKey: String,
    private val expand: suspend (W) -> Flow<C>,
    private val childCodec: WorkItemCodec<C>,
    private val concurrency: Int?,
    private val body: SubFlow<C>,
) {
    fun join() {}

    fun <A> reduce(initial: A, combine: (A, C) -> A, onComplete: suspend (W, A) -> Unit) {
        owner.replaceStep(
            index,
            FanIn(id, childKey, expand, childCodec, concurrency, body, initial, combine, onComplete),
        )
    }

    fun collect(onComplete: suspend (W, List<C>) -> Unit) {
        reduce(emptyList(), { acc, c -> acc + c }, onComplete)
    }
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
