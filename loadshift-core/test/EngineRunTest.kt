package loadshift.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Serializable
private data class EngineOrder(var id: String, var total: Int = 0, var note: String? = null) : WorkItem {
    override val key get() = id
}

@Serializable
private data class EngineLine(var id: String, var qty: Int = 1) : WorkItem {
    override val key get() = id
}

@OptIn(EngineApi::class)
class EngineRunTest {

    private fun EngineRun.type(topic: String) = EngineNames.jobType(workflow.key, topic)

    private fun EngineRun.generated(prefix: String) = jobTypes.single { it.substringAfter('/').startsWith(prefix) }

    private fun stepId(jobType: String, prefix: String) = jobType.substringAfter('/').removePrefix(prefix)

    private fun job(type: String, variables: JsonObject, retries: Int? = null, instance: String = "pi-1") =
        EngineJob("job-$instance", type, instance, variables, retries)

    @Test
    fun jobTypesMatchTheCompiledServiceTasks() {
        val wf = workflow<EngineOrder>("shop") {
            input(emptyList())
            task("charge") { }
            condition({ it.total > 0 }) { task("big") { } }
            loop({ false }) { task("spin") { } }
            timeout(1.seconds) { task("slow") { } }
            fanOut(expand = { emptyList<EngineLine>() }) {
                condition({ it.qty > 1 }) { task("bulk") { } }
            }.reduce(0, { acc, _ -> acc }) { _, _ -> }
        }
        val run = EngineRun(wf, RunConfig())
        val compiled = BpmnCompiler.compile(wf).flatMap { it.serviceTasks }.map { it.jobType }
        assertEquals(compiled.size, compiled.toSet().size)
        assertEquals(compiled.toSet(), run.jobTypes.toSet())
    }

    @Test
    fun completedTaskWritesTheUpdatedRootItem() = runTest {
        val wf = workflow<EngineOrder>("root-write") {
            input(emptyList())
            task("price") { it.total = 42 }
        }
        val run = EngineRun(wf, RunConfig())
        val outcome = assertIs<JobOutcome.Complete>(run.execute(job(run.type("price"), run.rootVariables(EngineOrder("o-1")))))
        assertEquals(JsonPrimitive(42), outcome.variables["total"])
        assertEquals(JsonPrimitive("o-1"), outcome.variables["id"])
    }

    @Test
    fun fieldsResetToTheirDefaultsAreWritten() = runTest {
        val wf = workflow<EngineOrder>("reset") {
            input(emptyList())
            task("clear") {
                it.total = 0
                it.note = null
            }
        }
        val run = EngineRun(wf, RunConfig())
        val vars = run.rootVariables(EngineOrder("o-1", total = 9, note = "rush"))
        val outcome = assertIs<JobOutcome.Complete>(run.execute(job(run.type("clear"), vars)))
        assertEquals(JsonPrimitive(0), outcome.variables["total"])
        assertEquals(JsonNull, outcome.variables["note"])
    }

    @Test
    fun rootVariablesCarryItemKeyAndRunId() {
        val wf = workflow<EngineOrder>("root-vars") { input(emptyList()) }
        val run = EngineRun(wf, RunConfig(), runId = "run-7")
        val vars = run.rootVariables(EngineOrder("o-1"))
        assertEquals("o-1", vars.getValue(EngineNames.ITEM_KEY).jsonPrimitive.content)
        assertEquals("run-7", vars.getValue(EngineNames.RUN_ID).jsonPrimitive.content)
    }

    @Test
    fun failingTaskRetriesWithDecreasingRetriesThenDeadLetters() = runTest {
        val wf = workflow<EngineOrder>("retries") {
            input(emptyList())
            task("charge", retry = RetryPolicy(maxAttempts = 3, baseDelay = 100.milliseconds, jitter = false)) { error("boom") }
        }
        val run = EngineRun(wf, RunConfig())
        val vars = run.rootVariables(EngineOrder("o-1"))

        val first = assertIs<JobOutcome.Retry>(run.execute(job(run.type("charge"), vars, retries = null)))
        assertEquals(2, first.retries)
        assertEquals(100.milliseconds, first.backoff)
        assertEquals("boom", first.message)

        val second = assertIs<JobOutcome.Retry>(run.execute(job(run.type("charge"), vars, retries = 2)))
        assertEquals(1, second.retries)
        assertEquals(200.milliseconds, second.backoff)

        assertIs<JobOutcome.Terminate>(run.execute(job(run.type("charge"), vars, retries = 1)))
        assertEquals(listOf(DeadLetter("o-1", "charge", "boom")), run.deadLetters())
    }

    @Test
    fun retryOnFalseExhaustsImmediately() = runTest {
        val wf = workflow<EngineOrder>("retry-on") {
            input(emptyList())
            task("validate", retry = RetryPolicy(maxAttempts = 5, retryOn = { it !is IllegalArgumentException })) {
                throw IllegalArgumentException("invalid")
            }
        }
        val run = EngineRun(wf, RunConfig())
        assertIs<JobOutcome.Terminate>(run.execute(job(run.type("validate"), run.rootVariables(EngineOrder("o-1")))))
    }

    @Test
    fun taskTimeoutIsARetryableFailure() = runTest {
        val wf = workflow<EngineOrder>("task-timeout") {
            input(emptyList())
            task("slow", timeout = 50.milliseconds, retry = RetryPolicy(maxAttempts = 2, jitter = false)) { delay(10.seconds) }
        }
        val run = EngineRun(wf, RunConfig())
        val outcome = assertIs<JobOutcome.Retry>(run.execute(job(run.type("slow"), run.rootVariables(EngineOrder("o-1")))))
        assertEquals("exceeded timeout 50ms", outcome.message)
    }

    @Test
    fun deadLetterRunsCompensationsInReverseAndRecordsTheirFailures() = runTest {
        val log = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<EngineOrder>("saga") {
            input(emptyList())
            task("reserve") { } compensate { log += "release" }
            task("charge") { } compensate { error("refund unavailable") }
            task("ship", retry = RetryPolicy.None) { error("no carrier") }
        }
        val run = EngineRun(wf, RunConfig())
        val vars = run.rootVariables(EngineOrder("o-1"))
        run.execute(job(run.type("reserve"), vars))
        run.execute(job(run.type("charge"), vars))

        assertIs<JobOutcome.Terminate>(run.execute(job(run.type("ship"), vars)))
        assertEquals(listOf("release"), log.toList())
        assertEquals(
            listOf(DeadLetter("o-1", "ship", "no carrier"), DeadLetter("o-1", "compensate_charge", "refund unavailable")),
            run.deadLetters(),
        )
    }

    @Test
    fun skipPolicyEndsTheItemWithoutCompensating() = runTest {
        val log = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<EngineOrder>("skip") {
            input(emptyList())
            task("reserve") { } compensate { log += "release" }
            task("ship", retry = RetryPolicy.None) { error("no carrier") }
        }
        val run = EngineRun(wf, RunConfig(onError = ErrorPolicy.Skip))
        val vars = run.rootVariables(EngineOrder("o-1"))
        run.execute(job(run.type("reserve"), vars))

        assertIs<JobOutcome.Terminate>(run.execute(job(run.type("ship"), vars)))
        assertEquals(emptyList(), log.toList())
        assertEquals(emptyList(), run.deadLetters())
        assertEquals(1, run.progress().skipped)
        run.rootFinished("pi-1", "o-1", completed = true)
        assertEquals(0, run.progress().done)
    }

    @Test
    fun failPolicyAbortsTheRun() = runTest {
        val wf = workflow<EngineOrder>("fail") {
            input(emptyList())
            task("ship", retry = RetryPolicy.None) { error("no carrier") }
        }
        val run = EngineRun(wf, RunConfig(onError = ErrorPolicy.Fail))
        val outcome = assertIs<JobOutcome.Abort>(run.execute(job(run.type("ship"), run.rootVariables(EngineOrder("o-1")))))
        assertEquals("no carrier", outcome.cause.message)
        assertEquals(1, run.progress().failed)
    }

    @Test
    fun unknownJobTypeAborts() = runTest {
        val run = EngineRun(workflow<EngineOrder>("unknown") { input(emptyList()) }, RunConfig())
        assertIs<JobOutcome.Abort>(run.execute(job("unknown/missing", JsonObject(emptyMap()))))
    }

    @Test
    fun finishedRootsCountAsDoneAndCheckpointUnlessTerminated() = runTest {
        val store = InMemoryCheckpointStore()
        val wf = workflow<EngineOrder>("finish") {
            input(emptyList())
            task("ship", retry = RetryPolicy.None) { if (it.id == "bad") error("rejected") }
        }
        val run = EngineRun(wf, RunConfig(checkpoints = store))
        run.execute(job(run.type("ship"), run.rootVariables(EngineOrder("bad")), instance = "pi-bad"))

        run.rootFinished("pi-good", "good", completed = true)
        run.rootFinished("pi-bad", "bad", completed = true)
        run.rootFinished("pi-gone", "gone", completed = false)

        assertEquals(1, run.progress().done)
        assertTrue(store.isComplete("finish", "good"))
        assertFalse(store.isComplete("finish", "bad"))
        assertFalse(store.isComplete("finish", "gone"))
    }

    @Test
    fun admitAppliesDedupeAndCheckpointsWhenResuming() = runTest {
        val store = InMemoryCheckpointStore().apply { markComplete("admit", "done") }
        val wf = workflow<EngineOrder>("admit") { input(emptyList()) }

        val resuming = EngineRun(wf, RunConfig(checkpoints = store))
        val seen = HashSet<String>()
        assertTrue(resuming.admit(EngineOrder("a"), seen))
        assertFalse(resuming.admit(EngineOrder("a"), seen))
        assertFalse(resuming.admit(EngineOrder("done"), seen))
        assertEquals(Progress(seeded = 1, skipped = 2), resuming.progress())

        val fresh = EngineRun(wf, RunConfig(checkpoints = store, resume = false))
        assertTrue(fresh.admit(EngineOrder("done"), null))
    }

    @Test
    fun conditionReportsThePredicateResult() = runTest {
        val wf = workflow<EngineOrder>("condition") {
            input(emptyList())
            condition({ it.total > 10 }) { task("big") { } }
        }
        val run = EngineRun(wf, RunConfig())
        val type = run.generated("decision_")
        val result = EngineNames.result(stepId(type, "decision_"))

        val big = assertIs<JobOutcome.Complete>(run.execute(job(type, run.rootVariables(EngineOrder("o-1", total = 11)))))
        assertEquals(JsonPrimitive(true), big.variables[result])
        val small = assertIs<JobOutcome.Complete>(run.execute(job(type, run.rootVariables(EngineOrder("o-2", total = 1)))))
        assertEquals(JsonPrimitive(false), small.variables[result])
    }

    @Test
    fun loopCountsIterationsAndDeadLettersBeyondTheLimit() = runTest {
        val wf = workflow<EngineOrder>("loop") {
            input(emptyList())
            loop({ it.total < 100 }) { task("spin") { } }
        }
        val run = EngineRun(wf, RunConfig(maxLoopIterations = 2))
        val type = run.generated("decision_")
        val id = stepId(type, "decision_")
        val vars = run.rootVariables(EngineOrder("o-1"))

        val first = assertIs<JobOutcome.Complete>(run.execute(job(type, vars)))
        assertEquals(JsonPrimitive(true), first.variables[EngineNames.result(id)])
        assertEquals(JsonPrimitive(1), first.variables[EngineNames.iterations(id)])

        val exhausted = JsonObject(vars + (EngineNames.iterations(id) to JsonPrimitive(2)))
        assertIs<JobOutcome.Terminate>(run.execute(job(type, exhausted)))
        assertEquals(listOf(DeadLetter("o-1", "loop_$id", "exceeded maxLoopIterations=2")), run.deadLetters())

        val done = assertIs<JobOutcome.Complete>(run.execute(job(type, run.rootVariables(EngineOrder("o-2", total = 100)))))
        assertEquals(JsonPrimitive(false), done.variables[EngineNames.result(id)])
        assertEquals(JsonPrimitive(0), done.variables[EngineNames.iterations(id)])
    }

    @Test
    fun expandEmitsChildrenWithKeysAndLineage() = runTest {
        val wf = workflow<EngineOrder>("expand") {
            input(emptyList())
            fanOut(expand = { o -> listOf(EngineLine("${o.id}-1"), EngineLine("${o.id}-2", qty = 3)) }) {
                task("pick") { }
            }
        }
        val run = EngineRun(wf, RunConfig())
        val type = run.generated("expand_")
        val items = EngineNames.items(stepId(type, "expand_"))

        val outcome = assertIs<JobOutcome.Complete>(run.execute(job(type, run.rootVariables(EngineOrder("o-1", note = "rush")))))
        val children = outcome.variables.getValue(items).jsonArray
        assertEquals(listOf("o-1-1", "o-1-2"), children.map { it.jsonObject.getValue(EngineNames.ITEM_KEY).jsonPrimitive.content })
        assertEquals(JsonPrimitive(3), children[1].jsonObject["qty"])
        val lineage = Json.parseToJsonElement(children[0].jsonObject.getValue(EngineNames.PARENTS).jsonPrimitive.content).jsonArray
        assertEquals(JsonPrimitive("o-1"), lineage.single().jsonObject["id"])
        assertEquals(JsonPrimitive("rush"), lineage.single().jsonObject["note"])
        assertEquals(2, run.progress().expanded)
    }

    @Test
    fun childTaskUsesItsOwnItemVariableAndSeesParentsAndLogPath() = runTest {
        val entries = Collections.synchronizedList(mutableListOf<LogEntry>())
        val sink = object : LogSink {
            override suspend fun write(entry: LogEntry) {
                entries += entry
            }
        }
        val parentSeen = CompletableDeferred<String>()
        val wf = workflow<EngineOrder>("child") {
            input(emptyList())
            fanOut(expand = { o -> listOf(EngineLine("${o.id}-1")) }, context = { it }) {
                task("pick") { line ->
                    line.qty = 5
                    parentSeen.complete(context().id)
                    log("picked")
                }
            }
        }
        val run = EngineRun(wf, RunConfig(logSink = sink))
        val expandType = run.generated("expand_")
        val fanId = stepId(expandType, "expand_")
        val expanded = assertIs<JobOutcome.Complete>(run.execute(job(expandType, run.rootVariables(EngineOrder("o-1")))))
        val child = expanded.variables.getValue(EngineNames.items(fanId)).jsonArray.single().jsonObject
        val itemVariable = EngineNames.item(fanId)
        val childVariables = JsonObject(
            mapOf("id" to JsonPrimitive("o-1"), "total" to JsonPrimitive(7), itemVariable to child),
        )

        val outcome = assertIs<JobOutcome.Complete>(run.execute(job(run.type("pick"), childVariables, instance = "child-1")))

        assertEquals(setOf(itemVariable), outcome.variables.keys)
        val written = outcome.variables.getValue(itemVariable).jsonObject
        assertEquals(JsonPrimitive("o-1-1"), written["id"])
        assertEquals(JsonPrimitive(5), written["qty"])
        assertEquals(child[EngineNames.ITEM_KEY], written[EngineNames.ITEM_KEY])
        assertEquals(child[EngineNames.PARENTS], written[EngineNames.PARENTS])
        assertEquals("o-1", parentSeen.await())
        val entry = entries.single()
        assertEquals(listOf("o-1"), entry.path)
        assertEquals("o-1-1", entry.itemKey)
        assertEquals("pick", entry.topic)
    }

    @Test
    fun reduceFoldsChildrenIntoTheParent() = runTest {
        val wf = workflow<EngineOrder>("reduce") {
            input(emptyList())
            fanOut(expand = { emptyList<EngineLine>() }) {
                task("pick") { }
            }.reduce(0, { acc, line -> acc + line.qty }) { order, total -> order.total = total }
        }
        val run = EngineRun(wf, RunConfig())
        val type = run.generated("reduce_")
        val children = JsonArray(
            listOf(
                buildJsonObject { put("id", "l-1"); put("qty", 2) },
                buildJsonObject { put("id", "l-2"); put("qty", 3) },
            ),
        )
        val vars = JsonObject(run.rootVariables(EngineOrder("o-1")) + (EngineNames.items(stepId(type, "reduce_")) to children))

        val outcome = assertIs<JobOutcome.Complete>(run.execute(job(type, vars)))
        assertEquals(JsonPrimitive(5), outcome.variables["total"])
    }

    @Test
    fun timeoutJobRecordsADeadLetterAndEndsTheItem() = runTest {
        val wf = workflow<EngineOrder>("timeout") {
            input(emptyList())
            timeout(5.seconds) { task("call") { } }
        }
        val run = EngineRun(wf, RunConfig())
        val type = run.generated("timeout_")

        assertEquals(JobOutcome.Complete(JsonObject(emptyMap())), run.execute(job(type, run.rootVariables(EngineOrder("o-1")))))
        assertEquals(listOf(DeadLetter("o-1", type.substringAfter('/'), "exceeded 5s")), run.deadLetters())
        run.rootFinished("pi-1", "o-1", completed = true)
        assertEquals(0, run.progress().done)
    }

    @Test
    fun maxAttemptsReflectTaskAndRunPolicies() {
        val wf = workflow<EngineOrder>("attempts") {
            input(emptyList())
            task("custom", retry = RetryPolicy(maxAttempts = 5)) { }
            task("standard") { }
            condition({ true }) { task("inner") { } }
        }
        val run = EngineRun(wf, RunConfig(retry = RetryPolicy(maxAttempts = 4)))
        assertEquals(5, run.maxAttempts(run.type("custom")))
        assertEquals(4, run.maxAttempts(run.type("standard")))
        assertEquals(1, run.maxAttempts(run.generated("decision_")))
        assertFailsWith<IllegalArgumentException> { run.maxAttempts("attempts/missing") }
    }

    @Test
    fun fanOutConcurrencyLimitsChildTasksPerWorker() = runTest {
        val running = AtomicInteger()
        val peak = AtomicInteger()
        val gate = CompletableDeferred<Unit>()
        val wf = workflow<EngineOrder>("limit") {
            input(emptyList())
            fanOut(expand = { emptyList<EngineLine>() }, concurrency = 1) {
                task("pick") {
                    peak.accumulateAndGet(running.incrementAndGet()) { a, b -> maxOf(a, b) }
                    gate.await()
                    running.decrementAndGet()
                }
            }
        }
        val run = EngineRun(wf, RunConfig())
        val itemVariable = EngineNames.item(stepId(run.generated("expand_"), "expand_"))
        fun childVariables(id: String) = JsonObject(mapOf(itemVariable to buildJsonObject { put("id", id) }))

        val first = async { run.execute(job(run.type("pick"), childVariables("l-1"), instance = "c-1")) }
        val second = async { run.execute(job(run.type("pick"), childVariables("l-2"), instance = "c-2")) }
        testScheduler.advanceUntilIdle()
        assertEquals(1, running.get())

        gate.complete(Unit)
        first.await()
        second.await()
        assertEquals(1, peak.get())
    }
}
