package loadshift.core

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Serializable
private data class IntroItem(var id: String) : WorkItem {
    override val key get() = id
}

@Serializable
private data class IntroChild(var label: String) : WorkItem {
    override val key get() = label
}

class ControlTest {

    private fun sample() = workflow<IntroItem>("intro") {
        input(emptyList())
        condition({ true }) {
            task("yes") {}
        } otherwise {
            task("no") {}
        }
        fanOut<IntroChild>(expand = { emptyList() }) {
            task("child") {}
        }
    }

    @Test
    fun describeFlowBuildsTree() {
        val tree = describeFlow(sample())
        assertEquals("workflow", tree.type)
        assertEquals("intro", tree.label)
        val flat = flatten(tree)
        assertTrue(flat.any { it.type == "task" && it.label == "yes" })
        assertTrue(flat.any { it.type == "task" && it.label == "no" })
        assertTrue(flat.any { it.type == "task" && it.label == "child" })
        assertTrue(flat.any { it.type == "if" })
        assertTrue(flat.any { it.type == "fanOut" })
    }

    @Test
    fun trackerSnapshotsInspector() = runTest {
        val tracker = RunTracker("test")
        val inspector = object : RunInspector {
            override fun state() = RunState.Running
            override fun progress() = Progress(seeded = 2, done = 1)
            override fun deadLetters() = listOf(DeadLetter("k", "t", "boom"))
            override suspend fun engineActive(): Long? = 7
        }
        val id = tracker.track(sample(), inspector)

        assertEquals("test", tracker.backendType)
        assertEquals(1, tracker.runs().size)
        val snap = tracker.run(id)
        assertNotNull(snap)
        assertEquals("intro", snap.workflowName)
        assertEquals(RunState.Running, snap.state)
        assertEquals(2, snap.progress.seeded)
        assertEquals(1, snap.progress.done)
        assertEquals(listOf(DeadLetter("k", "t", "boom")), snap.deadLetters)
        assertEquals(7, snap.engineActive)
        assertNotNull(tracker.structure(id))
        assertNull(tracker.run("missing"))
        assertNull(tracker.structure("missing"))
    }

    @Test
    fun trackerControlsRunViaHandle() = runTest {
        val tracker = RunTracker("test")
        val inspector = object : RunInspector {
            override fun state() = RunState.Running
            override fun progress() = Progress()
        }
        var started = false
        var paused = false
        var cancelled = false
        val control = object : RunHandle {
            override suspend fun start() { started = true }
            override fun progress() = Progress()
            override suspend fun pause() { paused = true }
            override suspend fun cancel() { cancelled = true }
            override suspend fun await() = RunResult(0, 0, 0, emptyList())
        }
        val id = tracker.track(sample(), inspector, control)

        assertTrue(tracker.start(id))
        assertTrue(tracker.pause(id))
        assertTrue(tracker.cancel(id))
        assertTrue(started)
        assertTrue(paused)
        assertTrue(cancelled)

        assertFalse(tracker.start("missing"))
        assertFalse(tracker.pause("missing"))
        assertFalse(tracker.cancel("missing"))
    }

    @Test
    fun trackerWithoutControlRejectsCommands() = runTest {
        val tracker = RunTracker("test")
        val inspector = object : RunInspector {
            override fun state() = RunState.Running
            override fun progress() = Progress()
        }
        val id = tracker.track(sample(), inspector)

        assertFalse(tracker.start(id))
        assertFalse(tracker.pause(id))
        assertFalse(tracker.cancel(id))
    }

    @Test
    fun trackerEvictsOldestCompletedRunsBeyondCapacity() = runTest {
        val tracker = RunTracker("test", maxEntries = 2)
        val completed = object : RunInspector {
            override fun state() = RunState.Completed
            override fun progress() = Progress()
        }
        val running = object : RunInspector {
            override fun state() = RunState.Running
            override fun progress() = Progress()
        }

        val first = tracker.track(sample(), completed)
        val second = tracker.track(sample(), completed)
        assertEquals(2, tracker.runs().size)

        val third = tracker.track(sample(), running)

        assertEquals(2, tracker.runs().size)
        assertNull(tracker.run(first))
        assertNotNull(tracker.run(second))
        assertNotNull(tracker.run(third))
    }

    @Test
    fun trackerKeepsRunningEntriesEvenBeyondCapacity() = runTest {
        val tracker = RunTracker("test", maxEntries = 1)
        val running = object : RunInspector {
            override fun state() = RunState.Running
            override fun progress() = Progress()
        }

        val first = tracker.track(sample(), running)
        val second = tracker.track(sample(), running)

        assertEquals(2, tracker.runs().size)
        assertNotNull(tracker.run(first))
        assertNotNull(tracker.run(second))
    }

    @Test
    fun inspectorDefaultsAreEmpty() = runTest {
        val inspector = object : RunInspector {
            override fun state() = RunState.Completed
            override fun progress() = Progress()
        }
        assertEquals(emptyList(), inspector.deadLetters())
        assertNull(inspector.engineActive())
    }

    private fun flatten(node: FlowNode): List<FlowNode> =
        listOf(node) + node.children.flatMap { flatten(it) }
}
