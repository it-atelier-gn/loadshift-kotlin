package loadshift.core

import kotlinx.coroutines.flow.flow

@LoadshiftDsl
class ChildFlowSpec1<P1 : WorkItem, W : WorkItem> @PublishedApi internal constructor(
    codec: WorkItemCodec<W>,
    baseKey: String,
    idgen: IdGen,
) : FlowSpec<W>(codec, baseKey, idgen) {

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend context(P1) (W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = ancestorStack()
                @Suppress("UNCHECKED_CAST")
                body(stack[0] as P1, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    inline fun <reified C : WorkItem> fanOut(
        noinline expand: suspend (W) -> List<C>,
        concurrency: Int? = null,
        noinline body: ChildFlowSpec2<P1, W, C>.() -> Unit,
    ) = fanOutInternal(
        workItemCodec<C>(),
        { w -> flow { for (c in expand(w)) emit(c) } },
        concurrency,
    ) { codec, key, idgen -> ChildFlowSpec2<P1, W, C>(codec, key, idgen).apply(body) }

    inline fun <reified C : WorkItem> fanOut(
        paginated: Paginated<W, C>,
        concurrency: Int? = null,
        noinline body: ChildFlowSpec2<P1, W, C>.() -> Unit,
    ) = fanOutInternal(workItemCodec<C>(), { w -> paginated.asFlow(w) }, concurrency) { codec, key, idgen ->
        ChildFlowSpec2<P1, W, C>(codec, key, idgen).apply(body)
    }
}

@LoadshiftDsl
class ChildFlowSpec2<P1 : WorkItem, P2 : WorkItem, W : WorkItem> @PublishedApi internal constructor(
    codec: WorkItemCodec<W>,
    baseKey: String,
    idgen: IdGen,
) : FlowSpec<W>(codec, baseKey, idgen) {

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend context(P1, P2) (W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = ancestorStack()
                @Suppress("UNCHECKED_CAST")
                body(stack[1] as P1, stack[0] as P2, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    inline fun <reified C : WorkItem> fanOut(
        noinline expand: suspend (W) -> List<C>,
        concurrency: Int? = null,
        noinline body: ChildFlowSpec3<P1, P2, W, C>.() -> Unit,
    ) = fanOutInternal(
        workItemCodec<C>(),
        { w -> flow { for (c in expand(w)) emit(c) } },
        concurrency,
    ) { codec, key, idgen -> ChildFlowSpec3<P1, P2, W, C>(codec, key, idgen).apply(body) }

    inline fun <reified C : WorkItem> fanOut(
        paginated: Paginated<W, C>,
        concurrency: Int? = null,
        noinline body: ChildFlowSpec3<P1, P2, W, C>.() -> Unit,
    ) = fanOutInternal(workItemCodec<C>(), { w -> paginated.asFlow(w) }, concurrency) { codec, key, idgen ->
        ChildFlowSpec3<P1, P2, W, C>(codec, key, idgen).apply(body)
    }
}

@LoadshiftDsl
class ChildFlowSpec3<P1 : WorkItem, P2 : WorkItem, P3 : WorkItem, W : WorkItem> @PublishedApi internal constructor(
    codec: WorkItemCodec<W>,
    baseKey: String,
    idgen: IdGen,
) : FlowSpec<W>(codec, baseKey, idgen) {

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend context(P1, P2, P3) (W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = ancestorStack()
                @Suppress("UNCHECKED_CAST")
                body(stack[2] as P1, stack[1] as P2, stack[0] as P3, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    inline fun <reified C : WorkItem> fanOut(
        noinline expand: suspend (W) -> List<C>,
        concurrency: Int? = null,
        noinline body: ChildFlowSpec4<P1, P2, P3, W, C>.() -> Unit,
    ) = fanOutInternal(
        workItemCodec<C>(),
        { w -> flow { for (c in expand(w)) emit(c) } },
        concurrency,
    ) { codec, key, idgen -> ChildFlowSpec4<P1, P2, P3, W, C>(codec, key, idgen).apply(body) }

    inline fun <reified C : WorkItem> fanOut(
        paginated: Paginated<W, C>,
        concurrency: Int? = null,
        noinline body: ChildFlowSpec4<P1, P2, P3, W, C>.() -> Unit,
    ) = fanOutInternal(workItemCodec<C>(), { w -> paginated.asFlow(w) }, concurrency) { codec, key, idgen ->
        ChildFlowSpec4<P1, P2, P3, W, C>(codec, key, idgen).apply(body)
    }
}

@LoadshiftDsl
class ChildFlowSpec4<P1 : WorkItem, P2 : WorkItem, P3 : WorkItem, P4 : WorkItem, W : WorkItem> @PublishedApi internal constructor(
    codec: WorkItemCodec<W>,
    baseKey: String,
    idgen: IdGen,
) : FlowSpec<W>(codec, baseKey, idgen) {

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend context(P1, P2, P3, P4) (W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = ancestorStack()
                @Suppress("UNCHECKED_CAST")
                body(stack[3] as P1, stack[2] as P2, stack[1] as P3, stack[0] as P4, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    inline fun <reified C : WorkItem> fanOut(
        noinline expand: suspend (W) -> List<C>,
        concurrency: Int? = null,
        noinline body: ChildFlowSpec5<P1, P2, P3, P4, W, C>.() -> Unit,
    ) = fanOutInternal(
        workItemCodec<C>(),
        { w -> flow { for (c in expand(w)) emit(c) } },
        concurrency,
    ) { codec, key, idgen -> ChildFlowSpec5<P1, P2, P3, P4, W, C>(codec, key, idgen).apply(body) }

    inline fun <reified C : WorkItem> fanOut(
        paginated: Paginated<W, C>,
        concurrency: Int? = null,
        noinline body: ChildFlowSpec5<P1, P2, P3, P4, W, C>.() -> Unit,
    ) = fanOutInternal(workItemCodec<C>(), { w -> paginated.asFlow(w) }, concurrency) { codec, key, idgen ->
        ChildFlowSpec5<P1, P2, P3, P4, W, C>(codec, key, idgen).apply(body)
    }
}

@LoadshiftDsl
class ChildFlowSpec5<P1 : WorkItem, P2 : WorkItem, P3 : WorkItem, P4 : WorkItem, P5 : WorkItem, W : WorkItem> @PublishedApi internal constructor(
    codec: WorkItemCodec<W>,
    baseKey: String,
    idgen: IdGen,
) : FlowSpec<W>(codec, baseKey, idgen) {

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend context(P1, P2, P3, P4, P5) (W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = ancestorStack()
                @Suppress("UNCHECKED_CAST")
                body(stack[4] as P1, stack[3] as P2, stack[2] as P3, stack[1] as P4, stack[0] as P5, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }
}
