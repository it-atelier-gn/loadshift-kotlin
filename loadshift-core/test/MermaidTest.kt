package loadshift.core

import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

@Serializable
private data class M(var id: String = "x") : WorkItem

class MermaidTest {
    @Test
    fun rendersFlowchartWithNodesBranchesAndFan() {
        val wf = workflow<M>("demo") {
            input(M())
            task("validate") { }
            condition({ true }) { task("a") { } } otherwise { task("b") { } }
            wait(5.minutes)
            fanOut(expand = { emptyList<M>() }) { task("child") { } }
                .reduce(0, combine = { acc, _ -> acc }) { _, _ -> }
        }
        val mermaid = wf.toMermaid()
        assertTrue(mermaid.startsWith("flowchart TD"), mermaid)
        assertTrue("[\"validate\"]" in mermaid, mermaid)
        assertTrue("[\"a\"]" in mermaid && "[\"b\"]" in mermaid, mermaid)
        assertTrue("-->|yes|" in mermaid && "-->|no|" in mermaid, mermaid)
        assertTrue("([\"wait\"])" in mermaid, mermaid)
        assertTrue("-.->|each child|" in mermaid, mermaid)
        assertTrue("([\"start\"])" in mermaid && "([\"end\"])" in mermaid, mermaid)
    }

    @Test
    fun rendersCaughtErrorsAndMessageTimeoutsAsDottedBranches() {
        val wf = workflow<M>("branches") {
            input(M())
            task("reserve") { }.catching<IllegalStateException> { task("backorder") { } }
            awaitMessage("paid", timeout = 5.minutes) onTimeout { task("remind") { } }
        }
        val mermaid = wf.toMermaid()
        assertTrue("-.->|IllegalStateException|" in mermaid, mermaid)
        assertTrue("[\"backorder\"]" in mermaid, mermaid)
        assertTrue("([\"message paid\"])" in mermaid, mermaid)
        assertTrue("-.->|5m|" in mermaid && "[\"remind\"]" in mermaid, mermaid)
    }
}
