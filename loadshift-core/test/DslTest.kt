package loadshift.core

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

private class Item : WorkItemBase() {
    var n: Int by required()
}

private class Child : WorkItemBase() {
    var v: Int by required()
}

private fun item(n: Int) = Item().apply { this.n = n }

private fun richWorkflow(): Workflow<Item> = workflow("wf") {
    items(item(1))
    key { it.n.toString() }
    task("a") { }
    ifThen({ it.n > 0 }) { task("b") { } } elseThen { task("c") { } }
    whileLoop({ it.n < 3 }) { task("d") { } }
    parallel {
        branch { task("e") { } }
        branch { task("f") { } }
    }
    forEach<Child>(expand = { listOf(Child().apply { v = 0 }) }) {
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
        assertTrue(cond.predicate(item(5)))
    }

    @Test
    fun fanOutChildIsSeparateLevel() {
        val wf = richWorkflow()
        val seq = wf.root.step as Sequence<Item>
        val fanOut = seq.steps[4] as FanOut<Item, *>
        assertTrue("g" in fanOut.body.tasks.keys)
    }
}
