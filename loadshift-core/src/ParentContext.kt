package loadshift.core

import kotlinx.coroutines.currentCoroutineContext
import kotlin.coroutines.CoroutineContext

class ParentItemStack(val items: List<WorkItem>) : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<ParentItemStack>
    override val key get() = Key
    fun push(item: WorkItem) = ParentItemStack(listOf(item) + items)
}

@PublishedApi
internal suspend fun ancestorStack(): List<WorkItem> =
    currentCoroutineContext()[ParentItemStack.Key]?.items
        ?: error("parent item not available outside fanOut")
