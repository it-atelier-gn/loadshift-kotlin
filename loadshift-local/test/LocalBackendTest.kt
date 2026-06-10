package loadshift.local

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.datetime.Clock
import loadshift.core.ErrorPolicy
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.Start
import loadshift.core.TaskOptions
import loadshift.core.WorkItemBase
import loadshift.core.required
import loadshift.core.workflow
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

private class Cust(vars: MutableMap<String, Any?> = mutableMapOf()) : WorkItemBase(vars) {
    var id: String by required(variables)
    var n: Int by required(variables)
}

private class Kid(vars: MutableMap<String, Any?> = mutableMapOf()) : WorkItemBase(vars) {
    var label: String by required(variables)
}

private fun cust(id: String, n: Int = 0) = Cust(mutableMapOf("id" to id, "n" to n))

class LocalBackendTest {

    @Test
    fun nestedFanOutVisitsEveryChild() = runTest {
        val seen = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Cust>("nested") {
            items(listOf(cust("a"), cust("b")))
            forEach<Kid>(expand = { c -> listOf(Kid(mutableMapOf("label" to "${c.id}-1")), Kid(mutableMapOf("label" to "${c.id}-2"))) }) {
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
            items(listOf(cust("yes", 5), cust("no", -1)))
            ifThen({ it.n > 0 }) {
                task("pos") { results += "pos:${it.id}" }
            } elseThen {
                task("neg") { results += "neg:${it.id}" }
            }
        }
        LocalBackend().run(wf).await()
        assertEquals(setOf("pos:yes", "neg:no"), results.toSet())
    }

    @Test
    fun whileLoopRunsUntilGuardFalse() = runTest {
        val item = cust("x", 0)
        val wf = workflow<Cust>("loop") {
            items(listOf(item))
            whileLoop({ it.n < 3 }) {
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
            items(listOf(cust("x")))
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
            items((1..12).map { cust("$it") })
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
            items(listOf(cust("x")))
            task("flaky", TaskOptions(retry = RetryPolicy(maxAttempts = 3, baseDelay = 1.milliseconds, jitter = false))) {
                attempts.incrementAndGet()
                throw RuntimeException("boom")
            }
        }
        val result = LocalBackend().run(wf, RunConfig(onError = ErrorPolicy.DeadLetter)).await()
        assertEquals(3, attempts.get())
        assertEquals(1, result.failed)
        assertEquals(1, result.deadLetters.size)
        assertEquals("flaky", result.deadLetters[0].topic)
    }

    @Test
    fun dryRunRecordsTopicsWithoutExecuting() = runTest {
        val wf = workflow<Cust>("dry") {
            items(listOf(cust("x", 1)))
            task("a") { error("should not run") }
            ifThen({ it.n > 0 }) {
                task("b") { error("should not run") }
            }
        }
        assertEquals(listOf("a", "b"), LocalBackend().dryRun(wf))
    }

    @Test
    fun manualStartWaitsForTrigger() = runTest {
        val ran = AtomicBoolean(false)
        val wf = workflow<Cust>("manual") {
            items(listOf(cust("x")))
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
    fun scheduledStartDelaysExecution() = runTest {
        val ran = AtomicBoolean(false)
        val wf = workflow<Cust>("sched") {
            items(listOf(cust("x")))
            task("t") { ran.set(true) }
        }
        val handle = LocalBackend().run(wf, RunConfig(start = Start.At(Clock.System.now() + 80.milliseconds)))
        handle.await()
        assertTrue(ran.get())
    }
}
