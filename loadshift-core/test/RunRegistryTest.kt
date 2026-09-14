package loadshift.core

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

@Serializable
private data class Ticket(val id: String) : WorkItem {
    override val key get() = id
}

class RunRegistryTest {

    private val flow = workflow<Ticket>("tickets") {
        input(emptyList())
        task("close") { }
    }

    private fun record(id: String, startedSeconds: Long, state: RunState = RunState.Running, staleSeconds: Long = startedSeconds + 15) =
        RunRecord(
            id = id,
            worker = id.substringBefore(':'),
            backendType = "local",
            workflowKey = "tickets",
            workflowName = "tickets",
            state = state,
            startedAt = Instant.fromEpochSeconds(startedSeconds),
            updatedAt = Instant.fromEpochSeconds(startedSeconds),
            staleAt = Instant.fromEpochSeconds(staleSeconds),
            progress = Progress(),
            deadLetters = 0,
        )

    @Test
    fun inMemoryRegistryListsNewestFirstAndRemovesRecords() = runTest {
        val registry = InMemoryRunRegistry()
        registry.save(record("a:run-1", 10))
        registry.save(record("b:run-1", 30))
        registry.save(record("a:run-2", 20))

        assertEquals(listOf("b:run-1", "a:run-2"), registry.list(limit = 2).map { it.id })
        registry.remove("b:run-1")
        assertNull(registry.get("b:run-1"))
        assertEquals(listOf("a:run-2", "a:run-1"), registry.list().map { it.id })
        assertFailsWith<IllegalArgumentException> { registry.list(limit = 0) }
    }

    @Test
    fun onlyUnfinishedRunsPastTheirStaleTimeAreStale() {
        val now = Instant.fromEpochSeconds(100)
        assertTrue(record("a:run-1", 0, staleSeconds = 99).isStale(now))
        assertFalse(record("a:run-1", 0, staleSeconds = 100).isStale(now))
        assertFalse(record("a:run-1", 0, RunState.Completed, staleSeconds = 1).isStale(now))
    }

    @Test
    fun trackerPublishesItsRunsAndListsRunsOfOtherWorkers() = runTest {
        val registry = InMemoryRunRegistry()
        val tracker = RunTracker("test", registry = registry, worker = "w1", publishInterval = 1.hours)
        var state = RunState.Running
        val inspector = object : RunInspector {
            override fun state() = state
            override fun progress() = Progress(seeded = 3)
            override fun deadLetters() = listOf(DeadLetter("k", "t", "boom"))
        }

        val id = tracker.track(flow, inspector)
        tracker.publish()

        assertEquals("w1:run-1", id)
        val published = assertNotNull(registry.get(id))
        assertEquals("w1", published.worker)
        assertEquals("test", published.backendType)
        assertEquals(RunState.Running, published.state)
        assertEquals(Progress(seeded = 3), published.progress)
        assertEquals(1, published.deadLetters)
        assertEquals(published.updatedAt + 3.hours, published.staleAt)

        registry.save(record("w2:run-1", 5))
        assertEquals(listOf("w2:run-1"), tracker.registeredRuns().map { it.id })

        state = RunState.Completed
        tracker.publish()
        assertEquals(RunState.Completed, registry.get(id)?.state)
        registry.remove(id)
        tracker.publish()
        assertNull(registry.get(id))
    }

    @Test
    fun registryFailuresDoNotReachTheTracker() = runTest {
        val failing = object : RunRegistry {
            override suspend fun save(record: RunRecord) = error("disk full")
            override suspend fun list(limit: Int): List<RunRecord> = error("disk full")
            override suspend fun get(id: String): RunRecord? = null
            override suspend fun remove(id: String) {}
        }
        val tracker = RunTracker("test", registry = failing, worker = "w1", publishInterval = 1.seconds)
        tracker.track(flow, object : RunInspector {
            override fun state() = RunState.Running
            override fun progress() = Progress()
        })

        tracker.publish()

        assertEquals(emptyList(), tracker.registeredRuns())
        assertEquals(1, tracker.runs().size)
    }

    @Test
    fun trackerWithoutRegistryKeepsPlainRunIds() {
        val tracker = RunTracker("test")
        val id = tracker.track(flow, object : RunInspector {
            override fun state() = RunState.Running
            override fun progress() = Progress()
        })
        assertEquals("run-1", id)
    }
}
