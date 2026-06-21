package loadshift.local

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.datetime.Clock
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import loadshift.core.ErrorPolicy
import loadshift.core.LogEntry
import loadshift.core.LogSink
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.Start
import loadshift.core.WorkItem
import loadshift.core.log
import loadshift.core.fanOut
import loadshift.core.task
import loadshift.core.workflow
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

@Serializable
private data class Cust(var id: String, var n: Int = 0) : WorkItem {
    override val key get() = id
}

@Serializable
private data class Kid(var label: String) : WorkItem {
    override val key get() = label
}

class LocalBackendTest {

    @Test
    fun nestedFanOutVisitsEveryChild() = runTest {
        val seen = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Cust>("nested") {
            input(listOf(Cust("a"), Cust("b")))
            fanOut(expand = { c -> listOf(Kid("${c.id}-1"), Kid("${c.id}-2")) }) {
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
            input(listOf(Cust("yes", 5), Cust("no", -1)))
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
        val item = Cust("x", 0)
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
            input(listOf(Cust("x")))
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
            input((1..12).map { Cust("$it") })
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
            input(listOf(Cust("x")))
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
            input(listOf(Cust("p")))
            fanOut(expand = { listOf(Kid("p-1")) }) {
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
            input(listOf(Cust("x", 1)))
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
            input(listOf(Cust("x")))
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
            input(listOf(Cust("order-1"), Cust("order-2"), Cust("order-3")))
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
            input(listOf(Cust("x")))
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
            input(listOf(Cust("p")))
            fanOut(expand = { listOf(Kid("p-1")) }) {
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
            input(listOf(Cust("x")))
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

    @Test
    fun contextExposesParentWhenRequested() = runTest {
        val seen = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Cust>("parent-access") {
            input(listOf(Cust("a", 42), Cust("b", 7)))
            fanOut(expand = { c -> listOf(Kid("${c.id}-1")) }, context = { it }) {
                task("read-parent") { child ->
                    val parent = context()
                    seen += "${child.label}:${parent.id}:${parent.n}"
                }
            }
        }
        LocalBackend().run(wf).await()
        assertEquals(setOf("a-1:a:42", "b-1:b:7"), seen.toSet())
    }

    @Test
    fun contextIsNullWhenNotConfigured() = runTest {
        val seen = Collections.synchronizedList(mutableListOf<String?>())
        val wf = workflow<Cust>("no-context") {
            input(listOf(Cust("a")))
            fanOut(expand = { c -> listOf(Kid("${c.id}-1")) }) {
                task("read") { child -> seen += context() }
            }
        }
        LocalBackend().run(wf).await()
        assertEquals(listOf<String?>(null), seen.toList())
    }

    @Test
    fun explicitContextTransformsParent() = runTest {
        val seen = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Cust>("explicit-context") {
            input(listOf(Cust("a", 42), Cust("b", 7)))
            fanOut(
                expand = { c -> listOf(Kid("${c.id}-1")) },
                context = { c -> "cust:${c.id}:${c.n}" },
            ) {
                task("read") { child ->
                    seen += "${child.label}:${context()}"
                }
            }
        }
        LocalBackend().run(wf).await()
        assertEquals(setOf("a-1:cust:a:42", "b-1:cust:b:7"), seen.toSet())
    }

    @Test
    fun nestedFanOutContextReturnsImmediateParent() = runTest {
        val seen = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Cust>("nested-context") {
            input(listOf(Cust("root", 1)))
            fanOut(expand = { c -> listOf(Kid("${c.id}-child")) }) {
                fanOut(expand = { k -> listOf(Cust("${k.label}-grandchild", 99)) }, context = { it }) {
                    task("read") { child ->
                        val parent = context()
                        seen += "${child.id}:${parent.label}"
                    }
                }
            }
        }
        LocalBackend().run(wf).await()
        assertEquals(listOf("root-child-grandchild:root-child"), seen.toList())
    }

    @Test
    fun cumulativeContextExposesGrandparent() = runTest {
        val seen = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Cust>("cumulative-context") {
            input(listOf(Cust("root", 1)))
            fanOut(expand = { c -> listOf(Kid("${c.id}-child")) }, context = { it }) {
                fanOut(
                    expand = { k -> listOf(Cust("${k.label}-grandchild", 99)) },
                    context = { kid, cust -> "${kid.label}@${cust.id}" },
                ) {
                    task("read") { child ->
                        seen += "${child.id}:${context()}"
                    }
                }
            }
        }
        LocalBackend().run(wf).await()
        assertEquals(listOf("root-child-grandchild:root-child@root"), seen.toList())
    }

    @Test
    fun cumulativeContextComposesThreeLevels() = runTest {
        val seen = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Cust>("cumulative-three") {
            input(listOf(Cust("root", 1)))
            fanOut(expand = { c -> listOf(Kid("${c.id}-l1")) }, context = { it.id }) {
                fanOut(
                    expand = { k -> listOf(Kid("${k.label}-l2")) },
                    context = { kid, rootId -> "$rootId>${kid.label}" },
                ) {
                    fanOut(
                        expand = { k -> listOf(Cust("${k.label}-l3", 0)) },
                        context = { kid, upper -> "$upper>${kid.label}" },
                    ) {
                        task("read") { child ->
                            seen += "${child.id}:${context()}"
                        }
                    }
                }
            }
        }
        LocalBackend().run(wf).await()
        assertEquals(listOf("root-l1-l2-l3:root>root-l1>root-l1-l2"), seen.toList())
    }
}
