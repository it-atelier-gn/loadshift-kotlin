package loadshift.demo

import loadshift.core.ErrorPolicy
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.WorkItemBase
import loadshift.core.workflow
import loadshift.local.LocalBackend
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration.Companion.milliseconds

class Customer : WorkItemBase() {
    var id: String by required()
}

class Contact : WorkItemBase() {
    var id: String by required()
    var email: String? by optional()
}

private fun contact(id: String, email: String?) = Contact().apply {
    this.id = id
    this.email = email
}

private fun fetchContacts(customerId: String): List<Contact> = listOf(
    contact("$customerId-a", "$customerId@example.com"),
    contact("$customerId-b", null),
    contact("$customerId-c", "bad"),
)

fun main() = runBlocking<Unit> {
    val customers = (1..3).map { n -> Customer().apply { id = "cust-$n" } }

    val cleanup = workflow<Customer>("contact-cleanup") {
        key { it.id }
        items(customers)
        forEach<Contact>(expand = { fetchContacts(it.id) }, concurrency = 4) {
            ifThen({ it.email == null }) {
                task("flag-missing") { println("  flag-missing ${it.id}") }
            } elseThen {
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
