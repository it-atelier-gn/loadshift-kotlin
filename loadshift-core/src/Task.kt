package loadshift.core

abstract class Task<W : WorkItemBase>(val topic: String) {
    abstract suspend fun execute(item: W)
}

@JvmInline
value class Topic<W : WorkItemBase>(val name: String)

fun <W : WorkItemBase> task(topic: String, body: suspend (W) -> Unit): Task<W> =
    object : Task<W>(topic) {
        override suspend fun execute(item: W) = body(item)
    }
