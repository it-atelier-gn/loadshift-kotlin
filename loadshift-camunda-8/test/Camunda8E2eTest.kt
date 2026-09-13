package loadshift.camunda8

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import loadshift.core.DeadLetter
import loadshift.core.ErrorPolicy
import loadshift.core.InMemoryCheckpointStore
import loadshift.core.Progress
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.RunResult
import loadshift.core.RunState
import loadshift.core.Start
import loadshift.core.WorkItem
import loadshift.core.fanOut
import loadshift.core.task
import loadshift.core.workflow
import org.junit.jupiter.api.Assumptions
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

@Serializable
private data class Customer(var id: String, var note: String? = null) : WorkItem {
    override val key get() = id
}

@Serializable
private data class Contact(var id: String, var label: String = "") : WorkItem {
    override val key get() = id
}

private const val IMAGE = "camunda/camunda:8.9.19"
private const val BASE_VARIABLE = "LOADSHIFT_C8_BASE"

private val h2Config = """
    camunda:
      security:
        authentication:
          method: "basic"
          unprotectedApi: true
        authorizations:
          enabled: false
      data:
        secondary-storage:
          type: rdbms
          rdbms:
            url: jdbc:h2:mem:camunda;DB_CLOSE_DELAY=-1
            username: sa
            password:
            flushInterval: PT0.5S
""".trimIndent()

private object Camunda8Engine {
    private val base: String? by lazy {
        System.getenv(BASE_VARIABLE)?.let { return@lazy it }
        if (!DockerClientFactory.instance().isDockerAvailable) return@lazy null
        val container = GenericContainer(IMAGE)
            .withExposedPorts(8080, 9600)
            .withCopyToContainer(Transferable.of(h2Config), "/usr/local/camunda/config/application.yaml")
            .waitingFor(
                Wait.forHttp("/v2/topology")
                    .forPort(8080)
                    .forStatusCode(200)
                    .withStartupTimeout(java.time.Duration.ofMinutes(3)),
            )
        container.start()
        Runtime.getRuntime().addShutdownHook(Thread { container.stop() })
        "http://${container.host}:${container.getMappedPort(8080)}"
    }

    fun baseOrSkip(): String = base ?: Assumptions.abort("$IMAGE tests require Docker or $BASE_VARIABLE")
}

class Camunda8E2eTest {

    @Test
    fun fullLoopThroughRealEngine() = e2e { base ->
        val seen = Collections.synchronizedSet(mutableSetOf<String>())
        val totals = Collections.synchronizedSet(mutableSetOf<String>())
        val wf = workflow<Customer>(uniqueName("loop")) {
            input(listOf(Customer("a"), Customer("b")))
            timeout(60.seconds) {
                task("stamp") { it.note = "ok:${it.id}" }
                wait(1.seconds)
            }
            awaitMessage("proceed")
            fanOut(expand = { c -> listOf(Contact("${c.id}-1"), Contact("${c.id}-2")) }, context = { it }) {
                task("process") { contact ->
                    contact.label = "labelled"
                    seen += "${contact.id}:${context().id}:${context().note}"
                }
                task("verify") { contact -> seen += "${contact.id}:${contact.label}" }
            }.reduce(0, combine = { acc, contact -> acc + contact.id.length }) { customer, total ->
                totals += "${customer.id}:$total"
            }
        }

        val backend = Camunda8Backend(base)
        val handle = backend.run(wf, RunConfig(maxConcurrency = 4))
        handle.send("proceed", "a")
        handle.send("proceed", "b")
        val result = handle.await()

        assertEquals(RunResult(done = 2, failed = 0, skipped = 0, deadLetters = emptyList()), result)
        assertEquals(
            setOf(
                "a-1:a:ok:a", "a-2:a:ok:a", "b-1:b:ok:b", "b-2:b:ok:b",
                "a-1:labelled", "a-2:labelled", "b-1:labelled", "b-2:labelled",
            ),
            seen.toSet(),
        )
        assertEquals(setOf("a:6", "b:6"), totals.toSet())
        val snapshot = backend.control.runs().single()
        assertEquals(RunState.Completed, snapshot.state)
        assertEquals(Progress(seeded = 2, expanded = 4, done = 2), snapshot.progress)
    }

    @Test
    fun deadLetterCompensatesAndEndsOnlyThatItem() = e2e { base ->
        val compensated = Collections.synchronizedList(mutableListOf<String>())
        val attempts = AtomicInteger()
        val wf = workflow<Customer>(uniqueName("saga")) {
            input(listOf(Customer("bad"), Customer("good")))
            task("charge") { } compensate { compensated += "refund:${it.id}" }
            task("ship") {
                if (it.id == "bad") {
                    attempts.incrementAndGet()
                    throw IllegalStateException("boom")
                }
            }
        }

        val retry = RetryPolicy(maxAttempts = 2, baseDelay = 100.milliseconds, jitter = false)
        val result = Camunda8Backend(base).run(wf, RunConfig(retry = retry)).await()

        assertEquals(2, attempts.get())
        assertEquals(RunResult(done = 1, failed = 0, skipped = 0, deadLetters = listOf(DeadLetter("bad", "ship", "boom"))), result)
        assertEquals(listOf("refund:bad"), compensated.toList())
    }

    @Test
    fun skippedChildLetsParentFinish() = e2e { base ->
        val after = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Customer>(uniqueName("skip")) {
            input(listOf(Customer("p")))
            fanOut(expand = { listOf(Contact("p-ok"), Contact("p-bad")) }) {
                task("check", retry = RetryPolicy.None) { if (it.id == "p-bad") error("rejected") }
            }
            task("after") { after += it.id }
        }

        val result = Camunda8Backend(base).run(wf, RunConfig(onError = ErrorPolicy.Skip)).await()

        assertEquals(RunResult(done = 1, failed = 0, skipped = 1, deadLetters = emptyList()), result)
        assertEquals(listOf("p"), after.toList())
    }

    @Test
    fun failPolicyFailsRunAndCancelsItsInstances() = e2e { base ->
        val wf = workflow<Customer>(uniqueName("fail")) {
            input(listOf(Customer("broken"), Customer("waiting")))
            task("inspect", retry = RetryPolicy.None) { if (it.id == "broken") error("fatal") }
            awaitMessage("never")
        }

        val backend = Camunda8Backend(base)
        val failure = runCatching { backend.run(wf, RunConfig(onError = ErrorPolicy.Fail)).await() }.exceptionOrNull()

        assertEquals("fatal", failure?.message)
        val snapshot = backend.control.runs().single()
        assertEquals(RunState.Failed, snapshot.state)
        assertEquals(1, snapshot.progress.failed)
        val client = Camunda8Client(base)
        eventually { client.instanceCount(wf.key) == 0L }
    }

    @Test
    fun timeoutsAndLoopLimitsBecomeDeadLetters() = e2e { base ->
        val wf = workflow<Customer>(uniqueName("limits")) {
            input(listOf(Customer("slow"), Customer("spinning")))
            condition({ it.id == "slow" }) {
                timeout(2.seconds) { wait(60.seconds) }
            } otherwise {
                loop({ true }) { task("spin") { } }
            }
            task("unreachable") { error("reached by ${it.id}") }
        }

        val result = Camunda8Backend(base).run(wf, RunConfig(maxLoopIterations = 3)).await()

        assertEquals(0, result.done)
        assertEquals(
            mapOf("slow" to "timeout", "spinning" to "loop"),
            result.deadLetters.associate { it.key.orEmpty() to it.topic.substringBefore('_') },
        )
    }

    @Test
    fun messagesReachChildrenAndBroadcastsReleaseEveryWaiter() = e2e { base ->
        val progressed = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Customer>(uniqueName("messages")) {
            input(listOf(Customer("a"), Customer("b")))
            fanOut(expand = { c -> listOf(Contact("${c.id}-1"), Contact("${c.id}-2")) }) {
                awaitMessage("go")
                task("child-done") { progressed += it.id }
            }
            awaitMessage("close")
            task("root-done") { progressed += it.id }
        }

        val handle = Camunda8Backend(base).run(wf)
        handle.send("go", "a-1")
        eventually { "a-1" in progressed }
        assertEquals(listOf("a-1"), progressed.toList())
        handle.broadcast("go")
        eventually { progressed.size == 4 }
        handle.broadcast("close")
        val result = handle.await()

        assertEquals(2, result.done)
        assertEquals(setOf("a-1", "a-2", "b-1", "b-2", "a", "b"), progressed.toSet())
    }

    @Test
    fun pausedRunStartsNothingUntilResumed() = e2e { base ->
        val processed = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Customer>(uniqueName("pause")) {
            input(listOf(Customer("a"), Customer("b"), Customer("c")))
            task("work") { processed += it.id }
        }

        val backend = Camunda8Backend(base)
        val handle = backend.run(wf, RunConfig(start = Start.Manual))
        handle.pause()
        handle.start()
        delay(3.seconds)
        assertEquals(emptyList(), processed.toList())
        assertEquals(RunState.Paused, backend.control.runs().single().state)

        handle.resume()
        val result = handle.await()

        assertEquals(3, result.done)
        assertEquals(setOf("a", "b", "c"), processed.toSet())
    }

    @Test
    fun checkpointsSkipItemsCompletedByAnEarlierRun() = e2e { base ->
        val store = InMemoryCheckpointStore()
        val processed = Collections.synchronizedList(mutableListOf<String>())
        val name = uniqueName("resume")
        fun flow(failing: String?) = workflow<Customer>(name) {
            input(listOf(Customer("a"), Customer("b")))
            task("work", retry = RetryPolicy.None) {
                processed += it.id
                if (it.id == failing) error("boom")
            }
        }

        val backend = Camunda8Backend(base)
        backend.run(flow(failing = "b"), RunConfig(checkpoints = store)).await()
        processed.clear()
        val second = backend.run(flow(failing = null), RunConfig(checkpoints = store)).await()

        assertEquals(listOf("b"), processed.toList())
        assertEquals(RunResult(done = 1, failed = 0, skipped = 1, deadLetters = emptyList()), second)
    }

    @Test
    fun longRunningTaskKeepsItsLock() = e2e { base ->
        val executions = AtomicInteger()
        val wf = workflow<Customer>(uniqueName("lock")) {
            input(listOf(Customer("long")))
            task("slow") {
                executions.incrementAndGet()
                delay(15.seconds)
            }
        }

        val result = Camunda8Backend(base).run(wf, RunConfig(lockDuration = 6.seconds, maxConcurrency = 2)).await()

        assertEquals(1, executions.get())
        assertEquals(1, result.done)
    }

    @Test
    fun detachedInstancesAreFinishedByAnAttachedWorker() = e2e { base ->
        val charged = Collections.synchronizedList(mutableListOf<String>())
        val refunded = Collections.synchronizedList(mutableListOf<String>())
        val name = uniqueName("handover")
        fun flow() = workflow<Customer>(name) {
            input(listOf(Customer("ok"), Customer("broken")))
            task("charge") { charged += it.id } compensate { refunded += it.id }
            awaitMessage("ship")
            task("ship", retry = RetryPolicy.None) { if (it.id == "broken") error("no carrier") }
        }

        val first = Camunda8Backend(base)
        val original = first.run(flow())
        eventually { charged.size == 2 }
        original.detach()
        assertEquals(RunState.Detached, first.control.runs().single().state)
        val client = Camunda8Client(base)
        eventually { client.instanceCount(flow().key) == 2L }

        val second = Camunda8Backend(base)
        val takeover = second.attach(flow())
        takeover.send("ship", "ok")
        takeover.send("ship", "broken")
        val result = takeover.await()

        assertEquals(RunResult(done = 1, failed = 0, skipped = 0, deadLetters = listOf(DeadLetter("broken", "ship", "no carrier"))), result)
        assertEquals(listOf("broken"), refunded.toList())
        assertEquals(2, second.control.runs().single().progress.seeded)
    }

    @Test
    fun workflowsSharingTopicNamesDoNotExecuteEachOthersJobs() = e2e { base ->
        val first = Collections.synchronizedSet(mutableSetOf<String>())
        val second = Collections.synchronizedSet(mutableSetOf<String>())
        val backend = Camunda8Backend(base)

        val one = backend.run(
            workflow<Customer>(uniqueName("shared-one")) {
                input(listOf(Customer("1a"), Customer("1b")))
                task("work") { first += it.id }
            },
        )
        val two = backend.run(
            workflow<Customer>(uniqueName("shared-two")) {
                input(listOf(Customer("2a"), Customer("2b")))
                task("work") { second += it.id }
            },
        )
        one.await()
        two.await()

        assertEquals(setOf("1a", "1b"), first.toSet())
        assertEquals(setOf("2a", "2b"), second.toSet())
    }

    private fun e2e(block: suspend (base: String) -> Unit) = runBlocking {
        val base = Camunda8Engine.baseOrSkip()
        purgeActiveInstances(base)
        withTimeout(TEST_TIMEOUT) { block(base) }
    }

    private companion object {
        val TEST_TIMEOUT = 180.seconds
    }
}

private fun uniqueName(prefix: String) = "e2e-$prefix-${System.nanoTime()}"

private suspend fun eventually(timeout: Duration = 60.seconds, condition: suspend () -> Boolean) {
    val started = TimeSource.Monotonic.markNow()
    while (!condition()) {
        assertTrue(started.elapsedNow() < timeout, "condition not met within $timeout")
        delay(200.milliseconds)
    }
}

private fun purgeActiveInstances(base: String) {
    val http = HttpClient.newHttpClient()
    val search = HttpRequest.newBuilder(URI("$base/v2/process-instances/search"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("""{"filter":{"state":"ACTIVE"},"page":{"limit":1000}}"""))
        .build()
    val body = http.send(search, HttpResponse.BodyHandlers.ofString()).body()
    val items = Json.parseToJsonElement(body).jsonObject["items"]?.jsonArray ?: return
    for (item in items) {
        val key = item.jsonObject["processInstanceKey"]?.jsonPrimitive?.content ?: continue
        http.send(
            HttpRequest.newBuilder(URI("$base/v2/process-instances/$key/cancellation"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build(),
            HttpResponse.BodyHandlers.discarding(),
        )
    }
}
