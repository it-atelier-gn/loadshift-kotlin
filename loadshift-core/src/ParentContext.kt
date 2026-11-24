package loadshift.core

import kotlinx.coroutines.currentCoroutineContext
import kotlin.coroutines.CoroutineContext

class ParentItemStack internal constructor(private val source: Lazy<List<WorkItem>>) : CoroutineContext.Element {
    constructor(items: List<WorkItem>) : this(lazyOf(items))

    companion object Key : CoroutineContext.Key<ParentItemStack>
    override val key get() = Key
    val items: List<WorkItem> get() = source.value
    fun push(item: WorkItem) = ParentItemStack(listOf(item) + items)
}

@PublishedApi
internal suspend fun ancestorStack(): List<WorkItem> =
    currentCoroutineContext()[ParentItemStack.Key]?.items
        ?: error("parent item not available outside fanOut")
