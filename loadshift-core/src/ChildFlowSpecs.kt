package loadshift.core

import kotlinx.coroutines.currentCoroutineContext
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
        body: suspend (P1, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p1 = stack[0] as P1
                body(p1, item)
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
        body: suspend (P2, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p2 = stack[0] as P2
                body(p2, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (P1, P2, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p1 = stack[1] as P1
                @Suppress("UNCHECKED_CAST")
                val p2 = stack[0] as P2
                body(p1, p2, item)
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
        body: suspend (P3, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p3 = stack[0] as P3
                body(p3, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (P2, P3, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p2 = stack[1] as P2
                @Suppress("UNCHECKED_CAST")
                val p3 = stack[0] as P3
                body(p2, p3, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (P1, P2, P3, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p1 = stack[2] as P1
                @Suppress("UNCHECKED_CAST")
                val p2 = stack[1] as P2
                @Suppress("UNCHECKED_CAST")
                val p3 = stack[0] as P3
                body(p1, p2, p3, item)
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
        body: suspend (P4, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p4 = stack[0] as P4
                body(p4, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (P3, P4, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p3 = stack[1] as P3
                @Suppress("UNCHECKED_CAST")
                val p4 = stack[0] as P4
                body(p3, p4, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (P2, P3, P4, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p2 = stack[2] as P2
                @Suppress("UNCHECKED_CAST")
                val p3 = stack[1] as P3
                @Suppress("UNCHECKED_CAST")
                val p4 = stack[0] as P4
                body(p2, p3, p4, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (P1, P2, P3, P4, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p1 = stack[3] as P1
                @Suppress("UNCHECKED_CAST")
                val p2 = stack[2] as P2
                @Suppress("UNCHECKED_CAST")
                val p3 = stack[1] as P3
                @Suppress("UNCHECKED_CAST")
                val p4 = stack[0] as P4
                body(p1, p2, p3, p4, item)
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
        body: suspend (P5, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p5 = stack[0] as P5
                body(p5, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (P4, P5, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p4 = stack[1] as P4
                @Suppress("UNCHECKED_CAST")
                val p5 = stack[0] as P5
                body(p4, p5, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (P3, P4, P5, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p3 = stack[2] as P3
                @Suppress("UNCHECKED_CAST")
                val p4 = stack[1] as P4
                @Suppress("UNCHECKED_CAST")
                val p5 = stack[0] as P5
                body(p3, p4, p5, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (P2, P3, P4, P5, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p2 = stack[3] as P2
                @Suppress("UNCHECKED_CAST")
                val p3 = stack[2] as P3
                @Suppress("UNCHECKED_CAST")
                val p4 = stack[1] as P4
                @Suppress("UNCHECKED_CAST")
                val p5 = stack[0] as P5
                body(p2, p3, p4, p5, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }

    fun task(
        topic: String,
        retry: RetryPolicy? = null,
        timeout: kotlin.time.Duration? = null,
        rateLimit: Rate? = null,
        body: suspend (P1, P2, P3, P4, P5, W) -> Unit,
    ) {
        val t = object : Task<W>(topic) {
            override suspend fun execute(item: W) {
                val stack = currentCoroutineContext()[ParentItemStack.Key]?.items
                    ?: error("parentItem not available outside fanOut")
                @Suppress("UNCHECKED_CAST")
                val p1 = stack[4] as P1
                @Suppress("UNCHECKED_CAST")
                val p2 = stack[3] as P2
                @Suppress("UNCHECKED_CAST")
                val p3 = stack[2] as P3
                @Suppress("UNCHECKED_CAST")
                val p4 = stack[1] as P4
                @Suppress("UNCHECKED_CAST")
                val p5 = stack[0] as P5
                body(p1, p2, p3, p4, p5, item)
            }
        }
        task(t, retry, timeout, rateLimit)
    }
}
