package loadshift.core

import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

@Serializable
private data class Item(var n: Int) : WorkItem {
    override val key get() = n.toString()
}

@Serializable
private data class Child(var v: Int) : WorkItem

private fun richWorkflow(): Workflow<Item> = workflow("wf") {
    input(Item(1))
    task("a") { }
    condition({ it.n > 0 }) { task("b") { } } otherwise { task("c") { } }
    loop({ it.n < 3 }) { task("d") { } }
    parallel {
        branch { task("e") { } }
        branch { task("f") { } }
    }
    fanOut(expand = { listOf(Child(0)) }) {
        task("g") { }
    }
}

class DslTest {
    @Test
    fun buildsExpectedTopLevelStructure() {
        val wf = richWorkflow()
        val seq = assertIs<Sequence<Item>>(wf.root.step)
        assertIs<Execute<Item>>(seq.steps[0])
        assertIs<Conditional<Item>>(seq.steps[1])
        assertIs<Loop<Item>>(seq.steps[2])
        assertIs<Parallel<Item>>(seq.steps[3])
        assertIs<FanOut<Item, *>>(seq.steps[4])
    }

    @Test
    fun registersTasksAndDecisionsAtCorrectLevel() {
        val wf = richWorkflow()
        assertTrue(wf.root.tasks.keys.containsAll(listOf("a", "b", "c", "d", "e", "f")))
        assertTrue("g" !in wf.root.tasks.keys)
        assertEquals(2, wf.root.decisions.size)
    }

    @Test
    fun conditionalCarriesBothBranches() {
        val wf = richWorkflow()
        val seq = wf.root.step as Sequence<Item>
        val cond = seq.steps[1] as Conditional<Item>
        assertTrue(cond.onFalse != null)
        assertTrue(cond.predicate(Item(5)))
    }

    @Test
    fun fanOutChildIsSeparateLevel() {
        val wf = richWorkflow()
        val seq = wf.root.step as Sequence<Item>
        val fanOut = seq.steps[4] as FanOut<Item, *>
        assertTrue("g" in fanOut.body.tasks.keys)
    }
}
