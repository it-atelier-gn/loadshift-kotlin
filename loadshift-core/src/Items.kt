package loadshift.core

enum class ItemState { Waiting, Running, Done, DeadLettered, Skipped, Cancelled }

data class ItemStatus(
    val key: String,
    val state: ItemState,
    val topic: String?,
    val deadLetters: List<DeadLetter>,
)

class FinishedItems(private val capacity: Int = DEFAULT_CAPACITY) {
    private val states = object : LinkedHashMap<String, ItemState>(16, 0.75f, false) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, ItemState>?): Boolean = size > capacity
    }

    init {
        require(capacity > 0) { "capacity must be positive, was $capacity" }
    }

    fun record(key: String, state: ItemState) {
        synchronized(states) {
            states.remove(key)
            states[key] = state
        }
    }

    operator fun get(key: String): ItemState? = synchronized(states) { states[key] }

    companion object {
        const val DEFAULT_CAPACITY = 10_000
    }
}
