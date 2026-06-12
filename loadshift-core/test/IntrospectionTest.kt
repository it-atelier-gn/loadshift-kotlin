package loadshift.core

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

private class IntroItem : WorkItemBase() {
    var id: String by required()
    override val key get() = id
}

private class IntroChild : WorkItemBase() {
    var label: String by required()
    override val key get() = label
}

class IntrospectionTest {

    private fun sample() = workflow<IntroItem>("intro") {
        items(emptyList())
        ifThen({ true }) {
            task("yes") {}
        } elseThen {
            task("no") {}
        }
        forEach<IntroChild>(expand = { emptyList() }) {
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
