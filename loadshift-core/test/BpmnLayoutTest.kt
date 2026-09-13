package loadshift.core

import kotlinx.serialization.Serializable
import org.camunda.bpm.model.bpmn.instance.BoundaryEvent
import org.camunda.bpm.model.bpmn.instance.FlowNode
import org.camunda.bpm.model.bpmn.instance.Gateway
import org.camunda.bpm.model.bpmn.instance.Process
import org.camunda.bpm.model.bpmn.instance.SequenceFlow
import org.camunda.bpm.model.bpmn.instance.ServiceTask
import org.camunda.bpm.model.bpmn.instance.SubProcess
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnEdge
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnShape
import org.camunda.bpm.model.bpmn.instance.dc.Bounds
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

@Serializable
private data class LayoutItem(var n: Int) : WorkItem

class BpmnLayoutTest {

    private fun shapes(process: CompiledProcess): Map<String, BpmnShape> =
        process.model.getModelElementsByType(BpmnShape::class.java)
            .filter { it.bpmnElement != null }
            .associateBy { it.bpmnElement.id }

    private fun mainShapes(process: CompiledProcess): Map<String, BpmnShape> {
        val ids = process.model.getModelElementById<Process>(process.key)
            .getChildElementsByType(FlowNode::class.java)
            .filterNot { it is SubProcess && it.triggeredByEvent() }
            .map { it.id }
            .toSet()
        return shapes(process).filterKeys { it in ids }
    }

    private fun overlaps(a: Bounds, b: Bounds): Boolean =
        a.x < b.x + b.width && a.x + a.width > b.x && a.y < b.y + b.height && a.y + a.height > b.y

    private fun contains(outer: Bounds, inner: Bounds): Boolean =
        inner.x >= outer.x && inner.y >= outer.y &&
            inner.x + inner.width <= outer.x + outer.width && inner.y + inner.height <= outer.y + outer.height

    @Test
    fun sequenceLaysOutOnAStraightBackbone() {
        val wf = workflow<LayoutItem>("seq") {
            input(emptyList())
            task("a") { }
            task("b") { }
            task("c") { }
        }
        val shapes = mainShapes(BpmnCompiler.compile(wf)[0])
        assertEquals(5, shapes.size)
        val centers = shapes.values.map { it.bounds.y + it.bounds.height / 2 }
        assertTrue(centers.max() - centers.min() < 1.0, "sequence nodes should share one row")
        val xs = shapes.values.map { it.bounds.x }
        assertEquals(xs.size, xs.toSet().size, "every node should sit in its own column")
    }

    @Test
    fun noTwoTopLevelShapesOverlap() {
        val wf = workflow<LayoutItem>("mix") {
            input(emptyList())
            condition({ it.n > 0 }) { task("hi") { } } otherwise { task("lo") { } }
            parallel {
                branch { task("p1") { } }
                branch { task("p2") { } }
            }
            loop({ it.n < 3 }) { task("again") { } }
            timeout(5.seconds) { task("bounded") { } }
        }
        val process = BpmnCompiler.compile(wf)[0]
        val boundaries = process.model.getModelElementsByType(BoundaryEvent::class.java).map { it.id }.toSet()
        val placed = (mainShapes(process) - boundaries) + (BpmnCompiler.TERMINATE_SCOPE_ID to shapes(process).getValue(BpmnCompiler.TERMINATE_SCOPE_ID))
        val entries = placed.entries.toList()
        for (i in entries.indices) for (j in i + 1 until entries.size) {
            assertTrue(!overlaps(entries[i].value.bounds, entries[j].value.bounds), "${entries[i].key} and ${entries[j].key} overlap")
        }
    }

    @Test
    fun boundaryEventSitsOnTheBottomBorderOfItsHost() {
        val wf = workflow<LayoutItem>("boundary") {
            input(emptyList())
            timeout(5.seconds) { task("bounded") { } }
            task("after") { }
        }
        val process = BpmnCompiler.compile(wf)[0]
        val all = shapes(process)
        val boundary = process.model.getModelElementsByType(BoundaryEvent::class.java).single()
        val host = all.getValue(boundary.attachedTo.id).bounds
        val event = all.getValue(boundary.id).bounds
        val centerX = event.x + event.width / 2
        val centerY = event.y + event.height / 2
        assertTrue(centerX > host.x && centerX < host.x + host.width, "boundary event should sit within the host's width")
        assertTrue(kotlin.math.abs(centerY - (host.y + host.height)) < 1.0, "boundary event should sit on the host's bottom border")
    }

    @Test
    fun branchesAreSeparatedVertically() {
        val wf = workflow<LayoutItem>("branch") {
            input(emptyList())
            condition({ it.n > 0 }) { task("hi") { } } otherwise { task("lo") { } }
        }
        val shapes = mainShapes(BpmnCompiler.compile(wf)[0])
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
        val bottom = mainShapes(process).values.maxOf { it.bounds.y + it.bounds.height }
        val flows = process.model.getModelElementById<Process>(process.key)
            .getChildElementsByType(SequenceFlow::class.java)
            .map { it.id }
            .toSet()
        val maxWaypointY = process.model.getModelElementsByType(BpmnEdge::class.java)
            .filter { it.bpmnElement.id in flows }
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
        val shapes = mainShapes(process)
        fun center(match: (String) -> Boolean) = shapes.entries.first { match(it.key) }.value.bounds.let { it.y + it.height / 2 }
        val spine = listOf(
            center { it == "start" },
            center { it.startsWith("decision_") },
            center { it.startsWith("gw") },
            center { it == "end" },
        )
        assertTrue(spine.max() - spine.min() < 1.0, "start, guard, gateway and end should share one row")
        assertTrue(kotlin.math.abs(center { it.contains("retry") } - center { it == "start" }) > 50.0, "loop body should sit off the spine")
    }

    @Test
    fun decisionExpandAndGatewayNamesAreReadable() {
        val wf = workflow<LayoutItem>("names") {
            input(emptyList())
            condition({ it.n > 0 }) { task("hi") { } } otherwise { task("lo") { } }
            loop({ it.n < 3 }) { task("again") { } }
            fanOut(expand = { emptyList<LayoutItem>() }) { task("leaf") { } }
        }
        val root = BpmnCompiler.compile(wf)[0]
        val taskNames = root.model.getModelElementsByType(ServiceTask::class.java).map { it.name }.toSet()
        assertTrue("evaluate condition" in taskNames)
        assertTrue("loop condition" in taskNames)
        assertTrue("expand children" in taskNames)
        assertTrue(root.model.getModelElementsByType(Gateway::class.java).all { it.name.isNullOrEmpty() })
    }

    @Test
    fun terminateScopeSitsBelowTheMainFlowAndContainsItsEvents() {
        val wf = workflow<LayoutItem>("terminate-layout") {
            input(emptyList())
            condition({ it.n > 0 }) { task("hi") { } } otherwise { task("lo") { } }
            loop({ it.n < 3 }) { task("again") { } }
        }
        val process = BpmnCompiler.compile(wf)[0]
        val all = shapes(process)
        val scope = all.getValue(BpmnCompiler.TERMINATE_SCOPE_ID).bounds
        val mainBottom = mainShapes(process).values.maxOf { it.bounds.y + it.bounds.height }
        assertTrue(scope.y > mainBottom, "terminate scope should sit below the main flow")
        for (inner in listOf("${BpmnCompiler.TERMINATE_SCOPE_ID}_start", "${BpmnCompiler.TERMINATE_SCOPE_ID}_end")) {
            assertTrue(contains(scope, all.getValue(inner).bounds), "$inner should sit inside the terminate scope")
        }
    }

    @Test
    fun timeoutScopeKeepsItsContentsInsideAndIsFollowedByTheRecordTask() {
        val wf = workflow<LayoutItem>("timeout-layout") {
            input(emptyList())
            task("before") { }
            timeout(5.seconds) {
                task("inside-a") { }
                task("inside-b") { }
            }
            task("after") { }
        }
        val process = BpmnCompiler.compile(wf)[0]
        val all = shapes(process)
        val scopeElement = process.model.getModelElementsByType(SubProcess::class.java).single { !it.triggeredByEvent() }
        val scope = all.getValue(scopeElement.id).bounds
        for (node in scopeElement.getChildElementsByType(FlowNode::class.java)) {
            assertTrue(contains(scope, all.getValue(node.id).bounds), "${node.id} should sit inside ${scopeElement.id}")
        }
        val record = process.serviceTasks.single { it.topic.startsWith("timeout_") }
        assertTrue(all.getValue(record.id).bounds.x > scope.x + scope.width, "record task should follow the timeout scope")
    }
}
