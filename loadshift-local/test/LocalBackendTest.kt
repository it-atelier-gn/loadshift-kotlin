package loadshift.local

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.datetime.Clock
import kotlinx.serialization.json.JsonPrimitive
import loadshift.core.ErrorPolicy
import loadshift.core.LogEntry
import loadshift.core.LogSink
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.Start
import loadshift.core.WorkItemBase
import loadshift.core.log
import loadshift.core.workflow
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

private class Cust : WorkItemBase() {
    var id: String by required()
    var n: Int by required()
    override val key get() = id
}

private class Kid : WorkItemBase() {
    var label: String by required()
    override val key get() = label
}

private fun cust(id: String, n: Int = 0) = Cust().apply {
    this.id = id
    this.n = n
}

private fun kid(label: String) = Kid().apply { this.label = label }

class LocalBackendTest {

    @Test
    fun nestedFanOutVisitsEveryChild() = runTest {
        val seen = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Cust>("nested") {
            input(listOf(cust("a"), cust("b")))
            fanOut<Kid>(expand = { c -> listOf(kid("${c.id}-1"), kid("${c.id}-2")) }) {
                task("touch") { k -> seen += k.label }
            }
        }
        val handle = LocalBackend().run(wf)
        val result = handle.await()
        assertEquals(setOf("a-1", "a-2", "b-1", "b-2"), seen.toSet())
        assertEquals(2, result.done)
        assertEquals(4, handle.progress().expanded)
    }

    @Test
    fun conditionalPicksBranch() = runTest {
        val results = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Cust>("cond") {
            input(listOf(cust("yes", 5), cust("no", -1)))
            condition({ it.n > 0 }) {
                task("pos") { results += "pos:${it.id}" }
            } otherwise {
                task("neg") { results += "neg:${it.id}" }
            }
        }
        LocalBackend().run(wf).await()
        assertEquals(setOf("pos:yes", "neg:no"), results.toSet())
    }

    @Test
    fun loopRunsUntilGuardFalse() = runTest {
        val item = cust("x", 0)
        val wf = workflow<Cust>("loop") {
            input(listOf(item))
            loop({ it.n < 3 }) {
                task("inc") { it.n = it.n + 1 }
            }
        }
        LocalBackend().run(wf).await()
        assertEquals(3, item.n)
    }

    @Test
    fun parallelRunsAllBranches() = runTest {
        val hits = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Cust>("par") {
            input(listOf(cust("x")))
            parallel {
                branch { task("e") { hits += "e" } }
                branch { task("f") { hits += "f" } }
            }
        }
        LocalBackend().run(wf).await()
        assertEquals(setOf("e", "f"), hits.toSet())
    }

    @Test
    fun concurrencyCapIsHonored() = runTest {
        val current = AtomicInteger()
        val max = AtomicInteger()
        val wf = workflow<Cust>("cap") {
            input((1..12).map { cust("$it") })
            task("work") {
                val c = current.incrementAndGet()
                max.updateAndGet { m -> maxOf(m, c) }
                delay(20)
                current.decrementAndGet()
            }
        }
        LocalBackend().run(wf, RunConfig(maxConcurrency = 3)).await()
        assertTrue(max.get() <= 3, "max concurrency was ${max.get()}")
    }

    @Test
    fun retriesThenDeadLetters() = runTest {
        val attempts = AtomicInteger()
        val wf = workflow<Cust>("retry") {
            input(listOf(cust("x")))
            task("flaky", retry = RetryPolicy(maxAttempts = 3, baseDelay = 1.milliseconds, jitter = false)) {
                attempts.incrementAndGet()
                throw RuntimeException("boom")
            }
        }
        val result = LocalBackend().run(wf, RunConfig(onError = ErrorPolicy.DeadLetter)).await()
        assertEquals(3, attempts.get())
        assertEquals(0, result.failed)
        assertEquals(1, result.deadLetters.size)
        assertEquals("flaky", result.deadLetters[0].topic)
        assertEquals("x", result.deadLetters[0].key)
    }

    @Test
    fun childDeadLetterCarriesItemKey() = runTest {
        val wf = workflow<Cust>("child-dlq") {
            input(listOf(cust("p")))
            fanOut<Kid>(expand = { listOf(kid("p-1")) }) {
                task("explode", retry = RetryPolicy(maxAttempts = 1)) { throw RuntimeException("boom") }
            }
        }
        val result = LocalBackend().run(wf, RunConfig(onError = ErrorPolicy.DeadLetter)).await()
        assertEquals(1, result.deadLetters.size)
        assertEquals("p-1", result.deadLetters[0].key)
    }

    @Test
    fun dryRunRecordsTopicsWithoutExecuting() = runTest {
        val wf = workflow<Cust>("dry") {
            input(listOf(cust("x", 1)))
            task("a") { error("should not run") }
            condition({ it.n > 0 }) {
                task("b") { error("should not run") }
            }
        }
        assertEquals(listOf("a", "b"), LocalBackend().dryRun(wf))
    }

    @Test
    fun manualStartWaitsForTrigger() = runTest {
        val ran = AtomicBoolean(false)
        val wf = workflow<Cust>("manual") {
            input(listOf(cust("x")))
            task("t") { ran.set(true) }
        }
        val handle = LocalBackend().run(wf, RunConfig(start = Start.Manual))
        delay(50)
        assertFalse(ran.get())
        handle.start()
        handle.await()
        assertTrue(ran.get())
    }

    @Test
    fun failPolicyCancelsInFlightSiblings() = runTest {
        val completed = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Cust>("fail-cancel") {
            input(listOf(cust("order-1"), cust("order-2"), cust("order-3")))
            task("work", retry = RetryPolicy.None) { c ->
                if (c.id == "order-2") error("boom")
                delay(200)
                completed += c.id
            }
        }
        val handle = LocalBackend().run(wf, RunConfig(onError = ErrorPolicy.Fail, maxConcurrency = 3))
        val result = runCatching { handle.await() }
        assertTrue(result.isFailure, "expected await() to fail")
        assertTrue(completed.isEmpty(), "in-flight siblings should be cancelled, completed=$completed")
    }

    @Test
    fun scheduledStartDelaysExecution() = runTest {
        val ran = AtomicBoolean(false)
        val wf = workflow<Cust>("sched") {
            input(listOf(cust("x")))
            task("t") { ran.set(true) }
        }
        val handle = LocalBackend().run(wf, RunConfig(start = Start.At(Clock.System.now() + 80.milliseconds)))
        handle.await()
        assertTrue(ran.get())
    }

    @Test
    fun logCapturesExecutionTreeContext() = runTest {
        val entries = Collections.synchronizedList(mutableListOf<LogEntry>())
        val sink = object : LogSink {
            override suspend fun write(entry: LogEntry) {
                entries += entry
            }
        }
        val wf = workflow<Cust>("logging") {
            input(listOf(cust("p")))
            fanOut<Kid>(expand = { listOf(kid("p-1")) }) {
                task("touch") { k -> log("processed", "label" to k.label) }
            }
        }
        LocalBackend().run(wf, RunConfig(logSink = sink)).await()

        assertEquals(1, entries.size)
        val entry = entries[0]
        assertEquals("logging", entry.workflowName)
        assertEquals("processed", entry.message)
        assertEquals("p-1", entry.itemKey)
        assertEquals("touch", entry.topic)
        assertEquals(listOf("p"), entry.path)
        assertEquals(JsonPrimitive("p-1"), entry.data["label"])
    }

    @Test
    fun cronStartReseedsOnEveryTick() = runTest {
        val runs = AtomicInteger(0)
        val wf = workflow<Cust>("cron") {
            input(listOf(cust("x")))
            task("t") { runs.incrementAndGet() }
        }
        val handle = LocalBackend().run(wf, RunConfig(start = Start.Cron("* * * * *")))

        val deadline = System.currentTimeMillis() + 2000
        while (runs.get() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10)
        }
        assertEquals(1, runs.get())

        handle.cancel()
        runCatching { handle.await() }
    }
}
