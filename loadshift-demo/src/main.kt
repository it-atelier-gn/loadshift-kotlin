package loadshift.demo

import kotlinx.coroutines.runBlocking
import loadshift.core.ErrorPolicy
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.TaskOptions
import loadshift.core.WorkItemBase
import loadshift.core.optional
import loadshift.core.required
import loadshift.core.workflow
import loadshift.local.LocalBackend
import kotlin.time.Duration.Companion.milliseconds

class Customer(vars: MutableMap<String, Any?> = mutableMapOf()) : WorkItemBase(vars) {
    var id: String by required(variables)
}

class Contact(vars: MutableMap<String, Any?> = mutableMapOf()) : WorkItemBase(vars) {
    var id: String by required(variables)
    var email: String? by optional(variables)
}

private fun fetchContacts(customerId: String): List<Contact> = listOf(
    Contact(mutableMapOf("id" to "$customerId-a", "email" to "$customerId@example.com")),
    Contact(mutableMapOf("id" to "$customerId-b", "email" to null)),
    Contact(mutableMapOf("id" to "$customerId-c", "email" to "bad")),
)

fun main() = runBlocking {
    val customers = (1..3).map { Customer(mutableMapOf("id" to "cust-$it")) }

    val cleanup = workflow<Customer>("contact-cleanup") {
        key { it.id }
        items(customers)
        forEach<Contact>(expand = { fetchContacts(it.id) }, concurrency = 4) {
            ifThen({ it.email == null }) {
                task("flag-missing") { println("  flag-missing ${it.id}") }
            } elseThen {
                task(
                    "cleanup",
                    TaskOptions(retry = RetryPolicy(maxAttempts = 3, baseDelay = 5.milliseconds, jitter = false)),
                ) {
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

    // Run the same workflow durably on a real engine instead of in-process:
    //   Camunda7Backend("http://localhost:8080/engine-rest").run(cleanup).await()
    //   Camunda8Backend("http://localhost:8080").run(cleanup).await()
}
