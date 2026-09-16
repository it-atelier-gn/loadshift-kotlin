package loadshift.camunda7

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import loadshift.core.DeadLetter
import loadshift.core.EngineNames
import loadshift.core.ErrorPolicy
import loadshift.core.InMemoryCheckpointStore
import loadshift.core.InMemoryDeadLetterStore
import loadshift.core.ItemState
import loadshift.core.MigrationResult
import loadshift.core.MetricNames
import loadshift.core.Metrics
import loadshift.core.UserTask
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
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
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
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

private class OutOfStock(id: String) : RuntimeException("$id is out of stock")

@Serializable
private data class Contact(var id: String, var label: String = "") : WorkItem {
    override val key get() = id
}

internal class Camunda7Engine(private val image: String, private val baseVariable: String) {
    private val base: String? by lazy {
        System.getenv(baseVariable)?.let { return@lazy it }
        if (!DockerClientFactory.instance().isDockerAvailable) return@lazy null
        val container = GenericContainer(image)
            .withExposedPorts(8080)
            .waitingFor(
                Wait.forHttp("/engine-rest/version")
                    .forPort(8080)
                    .forStatusCode(200)
                    .withStartupTimeout(java.time.Duration.ofMinutes(3)),
            )
        container.start()
        Runtime.getRuntime().addShutdownHook(Thread { container.stop() })
        "http://${container.host}:${container.getMappedPort(8080)}/engine-rest"
    }

    fun baseOrSkip(): String = base ?: Assumptions.abort("$image tests require Docker or $baseVariable")
}

private val camundaSeven = Camunda7Engine(
    System.getenv("LOADSHIFT_C7_IMAGE") ?: "camunda/camunda-bpm-platform:run-7.24.0",
    "LOADSHIFT_C7_BASE",
)
private val cibSeven = Camunda7Engine(
    System.getenv("LOADSHIFT_CIB7_IMAGE") ?: "cibseven/cibseven:run-2.2.0",
    "LOADSHIFT_CIB7_BASE",
)

class Camunda7E2eTest : Camunda7Scenarios(camundaSeven)

class CibSevenE2eTest : Camunda7Scenarios(cibSeven)

abstract class Camunda7Scenarios internal constructor(private val engine: Camunda7Engine) {

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

        val backend = Camunda7Backend(base)
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
        val result = Camunda7Backend(base).run(wf, RunConfig(retry = retry)).await()

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

        val result = Camunda7Backend(base).run(wf, RunConfig(onError = ErrorPolicy.Skip)).await()

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

        val backend = Camunda7Backend(base)
        val failure = runCatching { backend.run(wf, RunConfig(onError = ErrorPolicy.Fail)).await() }.exceptionOrNull()

        assertEquals("fatal", failure?.message)
        val snapshot = backend.control.runs().single()
        assertEquals(RunState.Failed, snapshot.state)
        assertEquals(1, snapshot.progress.failed)
        val client = Camunda7Client(base)
        eventually { client.processInstanceCount(wf.key) == 0L }
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

        val result = Camunda7Backend(base).run(wf, RunConfig(maxLoopIterations = 3)).await()

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

        val handle = Camunda7Backend(base).run(wf)
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
    fun messageDataTimeoutsAndCaughtErrorsTakeTheirBranches() = e2e { base ->
        val seen = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Customer>(uniqueName("branches")) {
            input(listOf(Customer("paid"), Customer("silent"), Customer("sold-out")))
            fanOut(expand = { c -> listOf(Contact("${c.id}-1")) }) {
                awaitMessage("confirm") { contact, data -> contact.label = data.getValue("by").jsonPrimitive.content }
                task("confirmed") { seen += "confirmed:${it.id}:${it.label}" }
            }
            awaitMessage("payment", timeout = 15.seconds) { customer, data ->
                customer.note = data.getValue("amount").jsonPrimitive.content
            } onTimeout {
                task("remind") { seen += "remind:${it.id}" }
            }
            task("reserve") { if (it.id == "sold-out") throw OutOfStock(it.id) }.catching<OutOfStock> {
                task("backorder") { seen += "backorder:${it.id}" }
            }
            task("ship") { seen += "ship:${it.id}:${it.note}" }
        }

        val handle = Camunda7Backend(base).run(wf, RunConfig(retry = RetryPolicy(maxAttempts = 3, baseDelay = 100.milliseconds)))
        handle.broadcast("confirm", buildJsonObject { put("by", "ops") })
        handle.send("payment", "paid", buildJsonObject { put("amount", "42") })
        handle.send("payment", "sold-out", buildJsonObject { put("amount", "7") })
        val result = handle.await()

        assertEquals(RunResult(done = 3, failed = 0, skipped = 0, deadLetters = emptyList()), result)
        assertEquals(
            setOf(
                "confirmed:paid-1:ops", "confirmed:silent-1:ops", "confirmed:sold-out-1:ops",
                "remind:silent", "backorder:sold-out",
                "ship:paid:42", "ship:silent:null", "ship:sold-out:7",
            ),
            seen.toSet(),
        )
    }

    @Test
    fun pausedRunStartsNothingUntilResumed() = e2e { base ->
        val processed = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Customer>(uniqueName("pause")) {
            input(listOf(Customer("a"), Customer("b"), Customer("c")))
            task("work") { processed += it.id }
        }

        val backend = Camunda7Backend(base)
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

        val backend = Camunda7Backend(base)
        backend.run(flow(failing = "b"), RunConfig(checkpoints = store)).await()
        processed.clear()
        val second = backend.run(flow(failing = null), RunConfig(checkpoints = store)).await()

        assertEquals(listOf("b"), processed.toList())
        assertEquals(RunResult(done = 1, failed = 0, skipped = 1, deadLetters = emptyList()), second)
    }

    @Test
    fun longRunningTaskKeepsItsLock() = e2e { base ->
        val executions = AtomicInteger()
        val started = TimeSource.Monotonic.markNow()
        val timeline = Collections.synchronizedList(mutableListOf<String>())
        val metrics = object : Metrics {
            override fun increment(name: String, attributes: Map<String, String>, amount: Long) {
                if (name == MetricNames.LOCK_EXTENSIONS) {
                    timeline += "${started.elapsedNow().inWholeMilliseconds}ms extension ${attributes[MetricNames.OUTCOME]}"
                }
            }

            override fun record(name: String, attributes: Map<String, String>, duration: Duration) {}
        }
        val wf = workflow<Customer>(uniqueName("lock")) {
            input(listOf(Customer("long")))
            task("slow") {
                timeline += "${started.elapsedNow().inWholeMilliseconds}ms execution ${executions.incrementAndGet()} starts"
                delay(25.seconds)
            }
        }

        val result = Camunda7Backend(base).run(wf, RunConfig(lockDuration = 10.seconds, maxConcurrency = 2, metrics = metrics)).await()

        assertEquals(1, executions.get(), timeline.joinToString("; "))
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

        val first = Camunda7Backend(base)
        val original = first.run(flow())
        eventually { charged.size == 2 }
        original.detach()
        assertEquals(RunState.Detached, first.control.runs().single().state)
        assertEquals(2L, Camunda7Client(base).processInstanceCount(flow().key))

        val second = Camunda7Backend(base)
        val takeover = second.attach(flow())
        takeover.send("ship", "ok")
        takeover.send("ship", "broken")
        val result = takeover.await()

        assertEquals(RunResult(done = 1, failed = 0, skipped = 0, deadLetters = listOf(DeadLetter("broken", "ship", "no carrier"))), result)
        assertEquals(listOf("broken"), refunded.toList())
        assertEquals(2, second.control.runs().single().progress.seeded)
    }

    @Test
    fun deadLettersAreRequeuedAfterTheCauseIsFixed() = e2e { base ->
        val store = InMemoryDeadLetterStore()
        val broken = AtomicBoolean(true)
        val finished = Collections.synchronizedSet(mutableSetOf<String>())
        val wf = workflow<Customer>(uniqueName("requeue")) {
            input(listOf(Customer("root-bad"), Customer("parent")))
            task("check", retry = RetryPolicy.None) {
                if (it.id == "root-bad" && broken.get()) error("root broken")
                finished += it.id
            }
            fanOut(expand = { c -> if (c.id == "parent") listOf(Contact("parent-kid")) else emptyList() }, context = { it }) {
                task("child", retry = RetryPolicy.None) { kid ->
                    if (broken.get()) error("child broken")
                    finished += "${kid.id}<${context().id}"
                }
            }
        }

        val backend = Camunda7Backend(base)
        val first = backend.run(wf, RunConfig(deadLetters = store)).await()
        assertEquals(setOf("check", "child"), first.deadLetters.map { it.topic }.toSet())
        val records = store.list(wf.key).records
        assertEquals(2, records.size)

        broken.set(false)
        val requeued = backend.requeue(wf, records, RunConfig(deadLetters = store)).await()

        assertEquals(RunResult(done = 2, failed = 0, skipped = 0, deadLetters = emptyList()), requeued)
        assertTrue("root-bad" in finished && "parent-kid<parent" in finished, finished.toString())
        assertTrue(store.list(wf.key).records.isEmpty())
    }

    @Test
    fun userTasksAreListedAndCompletedWithFormData() = e2e { base ->
        val finished = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Customer>(uniqueName("approval")) {
            input(listOf(Customer("a")))
            userTask("approve order", assignee = "ops", candidateGroups = listOf("reviewers")) { customer, form ->
                customer.note = form.getValue("decision").jsonPrimitive.content
            }
            task("finish") { finished += "${it.id}:${it.note}" }
        }
        val backend = Camunda7Backend(base)
        val handle = backend.run(wf)
        var open = emptyList<UserTask>()
        eventually {
            open = backend.userTasks(wf)
            open.isNotEmpty()
        }
        val task = open.single()

        assertEquals(UserTask(task.id, wf.key, "approve order", "a", "ops", listOf("reviewers")), task)
        assertTrue(backend.completeUserTask(task.id, JsonObject(mapOf("decision" to JsonPrimitive("approved")))))
        assertEquals(1, handle.await().done)
        assertEquals(listOf("a:approved"), finished.toList())
        assertEquals(false, backend.completeUserTask(task.id, JsonObject(emptyMap())))
    }

    @Test
    fun migrateMovesWaitingInstancesToTheLatestVersion() = e2e { base ->
        val name = uniqueName("versioned")
        val steps = Collections.synchronizedList(mutableListOf<String>())
        val first = workflow<Customer>(name) {
            version("1")
            input(listOf(Customer("a")))
            awaitMessage("go")
            task("finish") { steps += "finish-1:${it.id}" }
        }
        val second = workflow<Customer>(name) {
            version("2")
            input(emptyList())
            awaitMessage("go")
            task("audit") { steps += "audit:${it.id}" }
            task("finish") { steps += "finish-2:${it.id}" }
        }
        val backend = Camunda7Backend(base)
        val original = backend.run(first)
        eventually { original.item("a")?.state == ItemState.Running }
        original.detach()

        assertEquals(MigrationResult(1, emptyList()), backend.migrate(second))

        val takeover = backend.attach(second)
        takeover.send("go", "a")
        assertEquals(1, takeover.await().done)
        assertEquals(listOf("audit:a", "finish-2:a"), steps.toList())
    }

    @Test
    fun callRunsTheCalledWorkflowAndReturnsTheItemToTheCaller() = e2e { base ->
        val shipped = Collections.synchronizedList(mutableListOf<String>())
        val billing = workflow<Customer>(uniqueName("billing")) {
            input(emptyList())
            task("invoice") { it.note = "invoiced" }
            awaitMessage("paid")
            task("archive", retry = RetryPolicy.None) { if (it.id == "broken") error("archive down") }
        }
        val checkout = workflow<Customer>(uniqueName("checkout")) {
            input(listOf(Customer("fine"), Customer("broken")))
            call(billing)
            task("ship") { shipped += "${it.id}:${it.note}" }
        }

        val handle = Camunda7Backend(base).run(checkout)
        handle.send("paid", "fine")
        handle.send("paid", "broken")
        val result = handle.await()

        assertEquals(setOf("fine:invoiced", "broken:invoiced"), shipped.toSet())
        assertEquals(2, result.done)
        assertEquals(listOf("archive"), result.deadLetters.map { it.topic })
    }

    @Test
    fun signalReleasesWaitingInstancesOfEveryWorkflow() = e2e { base ->
        val signal = uniqueName("signal")
        fun flow(name: String) = workflow<Customer>(uniqueName(name)) {
            input(listOf(Customer("a")))
            awaitSignal(signal)
            task("finish") { }
        }
        val backend = Camunda7Backend(base)
        val first = backend.run(flow("signal-one"))
        val second = backend.run(flow("signal-two"))
        eventually { first.item("a")?.state == ItemState.Running && second.item("a")?.state == ItemState.Running }

        eventually {
            backend.signal(signal)
            first.progress().done == 1L && second.progress().done == 1L
        }

        assertEquals(1, first.await().done)
        assertEquals(1, second.await().done)
    }

    @Test
    fun tenantIdDeploysStartsWorksAndCorrelatesWithinTheTenant() = e2e { base ->
        val wf = workflow<Customer>(uniqueName("tenant")) {
            input(listOf(Customer("a")))
            awaitMessage("go")
            task("finish") { }
        }
        val handle = Camunda7Backend(base, tenantId = "acme").run(wf)
        eventually { handle.item("a")?.state == ItemState.Running }
        handle.send("go", "a")

        assertEquals(1, handle.await().done)
        assertEquals(1L, restCount("$base/process-definition/count?key=${wf.key}&tenantIdIn=acme"))
        assertEquals(0L, restCount("$base/process-definition/count?key=${wf.key}&withoutTenantId=true"))
        assertEquals(1L, restCount("$base/history/process-instance/count?processDefinitionKey=${wf.key}&tenantIdIn=acme"))
    }

    @Test
    fun cronScheduleSeedsFromTheEngineTimerUntilTheRunIsCancelled() = e2e { base ->
        val wf = workflow<Customer>(uniqueName("cron")) {
            input(listOf(Customer("a"), Customer("b")))
            task("work") { }
        }
        val client = Camunda7Client(base)
        val schedule = EngineNames.scheduleProcess(wf.key)
        val handle = Camunda7Backend(base).run(wf, RunConfig(start = Start.Cron("* * * * *")))

        eventually(timeout = 150.seconds) { handle.progress().done == 2L }
        eventually { client.processInstanceCount(schedule) == 1L }

        handle.cancel()

        eventually { client.processInstanceCount(schedule) == 0L }
    }

    @Test
    fun maxInFlightStartsTheNextInstanceWhenOneFinishes() = e2e { base ->
        val wf = workflow<Customer>(uniqueName("in-flight")) {
            input(listOf(Customer("a"), Customer("b"), Customer("c")))
            awaitMessage("go")
            task("finish") { }
        }
        val handle = Camunda7Backend(base).run(wf, RunConfig(maxInFlight = 2))
        eventually { handle.progress().seeded == 2L }
        delay(2.seconds)
        assertEquals(2L, handle.progress().seeded)

        handle.send("go", "a")
        eventually { handle.progress().seeded == 3L }
        handle.send("go", "b")
        handle.send("go", "c")

        assertEquals(3, handle.await().done)
    }

    @Test
    fun cancelItemCancelsOneWaitingInstance() = e2e { base ->
        val wf = workflow<Customer>(uniqueName("cancel-item")) {
            input(listOf(Customer("keep"), Customer("drop")))
            awaitMessage("go")
            task("finish") { }
        }
        val handle = Camunda7Backend(base).run(wf)
        eventually { handle.item("keep")?.state == ItemState.Running && handle.item("drop")?.state == ItemState.Running }

        assertTrue(handle.cancelItem("drop"))
        handle.send("go", "keep")
        val result = handle.await()

        assertEquals(RunResult(done = 1, failed = 0, skipped = 0, deadLetters = emptyList(), cancelled = 1), result)
        assertEquals(ItemState.Cancelled, handle.item("drop")?.state)
        assertEquals(ItemState.Done, handle.item("keep")?.state)
    }

    @Test
    fun workflowsSharingTopicNamesDoNotExecuteEachOthersJobs() = e2e { base ->
        val first = Collections.synchronizedSet(mutableSetOf<String>())
        val second = Collections.synchronizedSet(mutableSetOf<String>())
        val backend = Camunda7Backend(base)

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
        val base = engine.baseOrSkip()
        purgeRunningInstances(base)
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

private fun restCount(url: String): Long {
    val response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI(url)).GET().build(), HttpResponse.BodyHandlers.ofString())
    return Json.parseToJsonElement(response.body()).jsonObject.getValue("count").jsonPrimitive.content.toLong()
}

private fun purgeRunningInstances(base: String) {
    val http = HttpClient.newHttpClient()
    val list = http.send(
        HttpRequest.newBuilder(URI("$base/process-instance?maxResults=1000")).GET().build(),
        HttpResponse.BodyHandlers.ofString(),
    ).body()
    for (instance in Json.parseToJsonElement(list).jsonArray) {
        val id = instance.jsonObject["id"]?.jsonPrimitive?.content ?: continue
        http.send(
            HttpRequest.newBuilder(URI("$base/process-instance/$id?skipCustomListeners=true")).DELETE().build(),
            HttpResponse.BodyHandlers.discarding(),
        )
    }
}
