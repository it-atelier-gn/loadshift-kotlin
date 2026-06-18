package loadshift.demo

import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import loadshift.core.ErrorPolicy
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.WorkItem
import loadshift.core.fanOut
import loadshift.core.task
import loadshift.core.workflow
import loadshift.local.LocalBackend
import loadshift.web.ControlServer
import kotlin.time.Duration.Companion.milliseconds

@Serializable
class User(var id: String) : WorkItem {
    override val key get() = id
}

@Serializable
class Contact(var id: String, var email: String? = null) : WorkItem {
    override val key get() = id
}

private fun fetchContacts(customerId: String): List<Contact> = listOf(
    Contact("$customerId-a", "$customerId@example.com"),
    Contact("$customerId-b", null),
    Contact("$customerId-c", "bad"),
)

fun main(args: Array<String>) = runBlocking<Unit> {
    if (args.contains("--ui")) {
        uiDemo()
        return@runBlocking
    }

    val customers = (1..3).map { n -> User("cust-$n") }

    val cleanup = workflow<User>("contact-cleanup") {
        input(customers)
        fanOut(expand = { fetchContacts(it.id) }, concurrency = 4) {
            condition({ it.email == null }) {
                task("flag-missing") { println("  flag-missing ${it.id}") }
            } otherwise {
                task("cleanup", retry = RetryPolicy(maxAttempts = 3, baseDelay = 5.milliseconds, jitter = false)) {
                    if (it.email == "bad") throw IllegalStateException("invalid email for ${it.id}")
                    println("  cleanup ${it.id} <${it.email}>")
                }
            }
        }
    }

    println("== Dry run plan ==")
    LocalBackend().dryRun(cleanup).forEach { println("  $it") }

    println("== Execute on LocalBackend ==")
    val result = LocalBackend()
        .run(cleanup, RunConfig(maxConcurrency = 8, onError = ErrorPolicy.DeadLetter))
        .await()

    println("done=${result.done} failed=${result.failed} deadLetters=${result.deadLetters.size}")
    result.deadLetters.forEach { println("  DLQ topic=${it.topic} error=${it.error}") }
}

private suspend fun uiDemo() {
    val backend = LocalBackend()
    val server = ControlServer(backend, port = 8571).start()
    println("loadshift console: http://127.0.0.1:8571")

    val customers = (1..6).map { n -> User("cust-$n") }
    val cleanup = workflow<User>("contact-cleanup") {
        input(customers)
        fanOut(expand = { fetchContacts(it.id) }, concurrency = 2) {
            condition({ it.email == null }) {
                task("flag-missing") { delay(800) }
            } otherwise {
                task("cleanup", retry = RetryPolicy(maxAttempts = 2, baseDelay = 100.milliseconds, jitter = false)) {
                    delay(800)
                    if (it.email == "bad") throw IllegalStateException("invalid email for ${it.id}")
                }
            }
        }
    }

    val result = backend.run(cleanup, RunConfig(maxConcurrency = 4, onError = ErrorPolicy.DeadLetter)).await()
    println("run finished: done=${result.done} failed=${result.failed}; console stays up, Ctrl+C to exit")
    try {
        awaitCancellation()
    } finally {
        server.stop()
    }
}
