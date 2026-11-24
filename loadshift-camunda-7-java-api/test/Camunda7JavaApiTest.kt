package loadshift.camunda7.javaapi

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import loadshift.camunda7.CamundaValue
import loadshift.camunda7.CamundaVariables
import loadshift.core.DeadLetter
import loadshift.core.ItemState
import loadshift.core.MigrationResult
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.RunResult
import loadshift.core.UserTask
import loadshift.core.WorkItem
import loadshift.core.fanOut
import loadshift.core.task
import loadshift.core.workflow
import org.camunda.bpm.engine.ProcessEngine
import org.camunda.bpm.engine.impl.cfg.StandaloneInMemProcessEngineConfiguration
import org.camunda.bpm.engine.impl.jobexecutor.DefaultJobExecutor
import org.camunda.spin.plugin.impl.SpinProcessEnginePlugin
import java.util.Collections
import java.util.UUID
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

@Serializable
private data class Batch(var id: String, var items: List<Contact> = emptyList()) : WorkItem {
    override val key get() = id
}

private class OutOfStock(id: String) : RuntimeException("$id is out of stock")

class Camunda7JavaApiTest {

    @Test
    fun runsTimersMessagesAndFanOutOnTheEngineInTheSameJvm() = embedded { engine ->
        val seen = Collections.synchronizedSet(mutableSetOf<String>())
        val totals = Collections.synchronizedSet(mutableSetOf<String>())
        val wf = workflow<Customer>("loop") {
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
            }.reduce(0, combine = { acc, contact -> acc + contact.id.length }) { customer, total ->
                totals += "${customer.id}:$total"
            }
        }

        val handle = engine.backend().run(wf, RunConfig(maxConcurrency = 4))
        handle.send("proceed", "a")
        handle.send("proceed", "b")
        val result = handle.await()

        assertEquals(RunResult(done = 2, failed = 0, skipped = 0, deadLetters = emptyList()), result)
        assertEquals(setOf("a-1:a:ok:a", "a-2:a:ok:a", "b-1:b:ok:b", "b-2:b:ok:b"), seen.toSet())
        assertEquals(setOf("a:6", "b:6"), totals.toSet())
        assertEquals(0L, engine.processInstanceCount(wf.key))
    }

    @Test
    fun deadLetterCompensatesAndEndsOnlyThatItem() = embedded { engine ->
        val compensated = Collections.synchronizedList(mutableListOf<String>())
        val attempts = AtomicInteger()
        val wf = workflow<Customer>("saga") {
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
        val result = engine.backend().run(wf, RunConfig(retry = retry)).await()

        assertEquals(2, attempts.get())
        assertEquals(RunResult(done = 1, failed = 0, skipped = 0, deadLetters = listOf(DeadLetter("bad", "ship", "boom"))), result)
        assertEquals(listOf("refund:bad"), compensated.toList())
    }

    @Test
    fun messageDataTimeoutsAndCaughtErrorsTakeTheirBranches() = embedded { engine ->
        val seen = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Customer>("branches") {
            input(listOf(Customer("paid"), Customer("silent"), Customer("sold-out")))
            awaitMessage("payment", timeout = 3.seconds) { customer, data ->
                customer.note = data.getValue("amount").jsonPrimitive.content
            } onTimeout {
                task("remind") { seen += "remind:${it.id}" }
            }
            task("reserve") { if (it.id == "sold-out") throw OutOfStock(it.id) }.catching<OutOfStock> {
                task("backorder") { seen += "backorder:${it.id}" }
            }
            task("ship") { seen += "ship:${it.id}:${it.note}" }
        }

        val handle = engine.backend().run(wf)
        handle.send("payment", "paid", buildJsonObject { put("amount", "42") })
        handle.send("payment", "sold-out", buildJsonObject { put("amount", "7") })
        val result = handle.await()

        assertEquals(RunResult(done = 3, failed = 0, skipped = 0, deadLetters = emptyList()), result)
        assertEquals(
            setOf("remind:silent", "backorder:sold-out", "ship:paid:42", "ship:silent:null", "ship:sold-out:7"),
            seen.toSet(),
        )
    }

    @Test
    fun userTasksAreListedAndCompletedWithFormData() = embedded { engine ->
        val finished = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Customer>("approval") {
            input(listOf(Customer("a")))
            userTask("approve order", assignee = "ops", candidateGroups = listOf("reviewers")) { customer, form ->
                customer.note = form.getValue("decision").jsonPrimitive.content
            }
            task("finish") { finished += "${it.id}:${it.note}" }
        }
        val backend = engine.backend()
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
    fun migrateMovesWaitingInstancesToTheLatestVersion() = embedded { engine ->
        val steps = Collections.synchronizedList(mutableListOf<String>())
        val first = workflow<Customer>("versioned") {
            version("1")
            input(listOf(Customer("a")))
            awaitMessage("go")
            task("finish") { steps += "finish-1:${it.id}" }
        }
        val second = workflow<Customer>("versioned") {
            version("2")
            input(emptyList())
            awaitMessage("go")
            task("audit") { steps += "audit:${it.id}" }
            task("finish") { steps += "finish-2:${it.id}" }
        }
        val backend = engine.backend()
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
    fun callRunsTheCalledWorkflowAndReturnsTheItemToTheCaller() = embedded { engine ->
        val shipped = Collections.synchronizedList(mutableListOf<String>())
        val billing = workflow<Customer>("billing") {
            input(emptyList())
            task("invoice") { it.note = "invoiced" }
        }
        val checkout = workflow<Customer>("checkout") {
            input(listOf(Customer("fine"), Customer("other")))
            call(billing)
            task("ship") { shipped += "${it.id}:${it.note}" }
        }

        val result = engine.backend().run(checkout).await()

        assertEquals(2, result.done)
        assertEquals(setOf("fine:invoiced", "other:invoiced"), shipped.toSet())
    }

    @Test
    fun signalReleasesWaitingInstances() = embedded { engine ->
        val wf = workflow<Customer>("signal") {
            input(listOf(Customer("a")))
            awaitSignal("release")
            task("finish") { }
        }
        val backend = engine.backend()
        val handle = backend.run(wf)
        eventually { handle.item("a")?.state == ItemState.Running }

        eventually {
            backend.signal("release")
            handle.progress().done == 1L
        }

        assertEquals(1, handle.await().done)
    }

    @Test
    fun tenantScopesDeploymentsAndInstances() = runBlocking {
        val processEngine = TestEngine.start()
        try {
            val api = Camunda7JavaApi(processEngine, tenantId = "acme")
            val wf = workflow<Customer>("tenant") {
                input(listOf(Customer("a")))
                awaitMessage("go")
                task("finish") { }
            }
            val handle = api.backend().run(wf)
            eventually { handle.item("a")?.state == ItemState.Running }
            handle.send("go", "a")

            assertEquals(1, handle.await().done)
            val definition = processEngine.repositoryService.createProcessDefinitionQuery()
                .processDefinitionKey(wf.key)
                .singleResult()
            assertEquals("acme", definition.tenantId)
        } finally {
            processEngine.close()
        }
    }

    @Test
    fun runningAnUnchangedWorkflowAgainDeploysNoNewVersion() = runBlocking {
        val processEngine = TestEngine.start()
        try {
            fun build() = workflow<Customer>("redeploy") {
                input(listOf(Customer("a")))
                task("stamp") { }
                fanOut(expand = { c -> listOf(Contact("${c.id}-1")) }) { task("process") { } }
            }
            val backend = Camunda7JavaApi(processEngine).backend()
            assertEquals(1, backend.run(build()).await().done)
            assertEquals(1, backend.run(build()).await().done)
            val versions = processEngine.repositoryService.createProcessDefinitionQuery().list()
                .filter { it.key.startsWith("redeploy") }
                .groupBy({ it.key }, { it.version })
            assertEquals(2, versions.size, versions.toString())
            assertTrue(versions.values.all { it == listOf(1) }, versions.toString())
        } finally {
            processEngine.close()
        }
    }

    @Test
    fun largeBatchFansOutInWavesOfItsConcurrency() = runBlocking {
        val processEngine = TestEngine.start()
        try {
            val batch = Batch("batch", List(2_384) { Contact("item-$it", "x".repeat(200)) })
            val processed = Collections.synchronizedSet(mutableSetOf<String>())
            val wf = workflow<Batch>("batch-workflow") {
                input(listOf(batch))
                fanOut(expand = { b -> b.items }, concurrency = 10) {
                    task("process") { processed += it.id }
                }
            }
            val childKey = "batch-workflow_f1"
            val peak = AtomicInteger()
            val sampler = launch(Dispatchers.IO) {
                while (isActive) {
                    val running = processEngine.runtimeService.createProcessInstanceQuery().processDefinitionKey(childKey).count().toInt()
                    peak.accumulateAndGet(running) { a, b -> maxOf(a, b) }
                    delay(20.milliseconds)
                }
            }
            val result = withTimeout(LARGE_TIMEOUT) { Camunda7JavaApi(processEngine).backend().run(wf, RunConfig(maxConcurrency = 16)).await() }
            sampler.cancel()

            assertEquals(1, result.done)
            assertEquals(2_384, processed.size)
            assertTrue(peak.get() in 1..10, "peak ${peak.get()}")
        } finally {
            processEngine.close()
        }
    }

    @Test
    fun variablesKeepTheirTypesThroughTheEngine() {
        val values = mapOf(
            "flag" to CamundaVariables.encode(true),
            "count" to CamundaVariables.encode(3),
            "big" to CamundaVariables.encode(5_000_000_000L),
            "ratio" to CamundaVariables.encode(0.5),
            "text" to CamundaVariables.encode("hi"),
            "nothing" to CamundaVariables.encode(null),
            "doc" to CamundaVariables.encode(mapOf("a" to listOf(1, 2))),
        )

        val roundTrip = Camunda7JavaApi.fromEngine(Camunda7JavaApi.toEngine(values))

        assertEquals(values.mapValues { CamundaVariables.decode(it.value) }, roundTrip.mapValues { CamundaVariables.decode(it.value) })
        assertEquals("Json", roundTrip.getValue("doc").type)
        assertEquals(CamundaValue(JsonNull, "Null"), Camunda7JavaApi.fromEngine(null as org.camunda.bpm.engine.variable.value.TypedValue?))
    }

    private fun embedded(block: suspend (Camunda7JavaApi) -> Unit) = runBlocking {
        val processEngine = TestEngine.start()
        try {
            withTimeout(TEST_TIMEOUT) { block(Camunda7JavaApi(processEngine)) }
        } finally {
            processEngine.close()
        }
    }

    private companion object {
        val TEST_TIMEOUT = 120.seconds
        val LARGE_TIMEOUT = 600.seconds
    }
}

private object TestEngine {
    fun start(): ProcessEngine {
        val configuration = StandaloneInMemProcessEngineConfiguration()
        configuration.processEngineName = "loadshift-test-${UUID.randomUUID()}"
        configuration.jdbcUrl = "jdbc:h2:mem:${UUID.randomUUID()};DB_CLOSE_DELAY=-1"
        configuration.jdbcMaxActiveConnections = 64
        configuration.isJobExecutorActivate = true
        configuration.jobExecutor = DefaultJobExecutor().apply {
            waitTimeInMillis = 200
            maxWait = 1000
        }
        configuration.processEnginePlugins = mutableListOf(SpinProcessEnginePlugin())
        return configuration.buildProcessEngine()
    }
}

private suspend fun eventually(timeout: Duration = 60.seconds, condition: suspend () -> Boolean) {
    val started = TimeSource.Monotonic.markNow()
    while (!condition()) {
        assertTrue(started.elapsedNow() < timeout, "condition not met within $timeout")
        delay(200.milliseconds)
    }
}
