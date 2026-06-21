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
}
