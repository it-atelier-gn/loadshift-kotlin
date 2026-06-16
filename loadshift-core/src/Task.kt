package loadshift.core

abstract class Task<W : WorkItem>(val topic: String) {
    abstract suspend fun execute(item: W)
}

@JvmInline
value class Topic<W : WorkItem>(val name: String)

fun <W : WorkItem> task(topic: String, body: suspend (W) -> Unit): Task<W> =
    object : Task<W>(topic) {
        override suspend fun execute(item: W) = body(item)
    }
