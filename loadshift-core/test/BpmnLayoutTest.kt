package loadshift.core

import kotlinx.serialization.Serializable
import org.camunda.bpm.model.bpmn.instance.Gateway
import org.camunda.bpm.model.bpmn.instance.ServiceTask
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnEdge
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnShape
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Serializable
private data class LayoutItem(var n: Int) : WorkItem

class BpmnLayoutTest {

    private fun shapes(process: CompiledProcess): Map<String, BpmnShape> =
        process.model.getModelElementsByType(BpmnShape::class.java)
            .filter { it.bpmnElement != null }
            .associateBy { it.bpmnElement.id }

    private fun overlaps(a: BpmnShape, b: BpmnShape): Boolean {
        val ab = a.bounds
        val bb = b.bounds
        return ab.x < bb.x + bb.width && ab.x + ab.width > bb.x &&
            ab.y < bb.y + bb.height && ab.y + ab.height > bb.y
    }

    @Test
    fun sequenceLaysOutOnAStraightBackbone() {
        val wf = workflow<LayoutItem>("seq") {
            input(emptyList())
            task("a") { }
            task("b") { }
            task("c") { }
        }
        val shapes = shapes(BpmnCompiler.compile(wf)[0])
        assertEquals(5, shapes.size)
        val centers = shapes.values.map { it.bounds.y + it.bounds.height / 2 }
        assertTrue(centers.max() - centers.min() < 1.0, "sequence nodes should share one row")
        val xs = shapes.values.map { it.bounds.x }
        assertEquals(xs.size, xs.toSet().size, "every node should sit in its own column")
    }

    @Test
    fun noTwoShapesOverlap() {
        val wf = workflow<LayoutItem>("mix") {
            input(emptyList())
            condition({ it.n > 0 }) { task("hi") { } } otherwise { task("lo") { } }
            parallel {
                branch { task("p1") { } }
                branch { task("p2") { } }
            }
            loop({ it.n < 3 }) { task("again") { } }
        }
        val shapes = shapes(BpmnCompiler.compile(wf)[0]).values.toList()
        for (i in shapes.indices) for (j in i + 1 until shapes.size) {
            assertTrue(!overlaps(shapes[i], shapes[j]), "shapes ${i} and ${j} overlap")
        }
    }

    @Test
    fun branchesAreSeparatedVertically() {
        val wf = workflow<LayoutItem>("branch") {
            input(emptyList())
            condition({ it.n > 0 }) { task("hi") { } } otherwise { task("lo") { } }
        }
        val shapes = shapes(BpmnCompiler.compile(wf)[0])
        val hi = shapes.values.first { it.bpmnElement.id.contains("hi") }.bounds
        val lo = shapes.values.first { it.bpmnElement.id.contains("lo") }.bounds
        assertTrue(kotlin.math.abs(hi.y - lo.y) > 50.0, "branch arms should be on different rows")
    }

    @Test
    fun loopBackEdgeIsRoutedBelowTheNodes() {
        val wf = workflow<LayoutItem>("loop") {
            input(emptyList())
            loop({ it.n < 3 }) { task("retry") { } }
        }
        val process = BpmnCompiler.compile(wf)[0]
        val bottom = shapes(process).values.maxOf { it.bounds.y + it.bounds.height }
        val maxWaypointY = process.model.getModelElementsByType(BpmnEdge::class.java)
            .flatMap { it.waypoints }
            .maxOf { it.y }
        assertTrue(maxWaypointY > bottom, "loop-back edge should route beneath the nodes")
    }

    @Test
    fun loopHasNoRedundantGatewayAndAStraightSpine() {
        val wf = workflow<LayoutItem>("loop") {
            input(emptyList())
            loop({ it.n < 3 }) { task("retry") { } }
        }
        val process = BpmnCompiler.compile(wf)[0]
        assertEquals(1, process.model.getModelElementsByType(Gateway::class.java).size, "loop should emit a single split gateway")
        val shapes = shapes(process)
        fun center(match: String) = shapes.values.first { it.bpmnElement.id.contains(match) }.bounds.let { it.y + it.height / 2 }
        val spine = listOf("start", "decision", "gw", "end").map { center(it) }
        assertTrue(spine.max() - spine.min() < 1.0, "start, guard, gateway and end should share one row")
        assertTrue(kotlin.math.abs(center("retry") - center("start")) > 50.0, "loop body should sit off the spine")
    }

    @Test
    fun decisionExpandAndGatewayNamesAreReadable() {
        val wf = workflow<LayoutItem>("names") {
            input(emptyList())
            condition({ it.n > 0 }) { task("hi") { } } otherwise { task("lo") { } }
            loop({ it.n < 3 }) { task("again") { } }
            fanOut<LayoutItem>(expand = { emptyList() }) { task("leaf") { } }
        }
        val root = BpmnCompiler.compile(wf)[0]
        val taskNames = root.model.getModelElementsByType(ServiceTask::class.java).map { it.name }.toSet()
        assertTrue("evaluate condition" in taskNames)
        assertTrue("loop guard" in taskNames)
        assertTrue("expand children" in taskNames)
        assertTrue(root.model.getModelElementsByType(Gateway::class.java).all { it.name.isNullOrEmpty() })
    }
}
