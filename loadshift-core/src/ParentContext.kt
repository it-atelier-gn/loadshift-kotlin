package loadshift.core

import kotlin.coroutines.CoroutineContext

class ParentItemStack(val items: List<WorkItem>) : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<ParentItemStack>
    override val key get() = Key
    fun push(item: WorkItem) = ParentItemStack(listOf(item) + items)
}
