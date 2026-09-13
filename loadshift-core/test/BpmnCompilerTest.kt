package loadshift.core

import kotlinx.serialization.Serializable
import org.camunda.bpm.model.bpmn.Bpmn
import org.camunda.bpm.model.bpmn.instance.BoundaryEvent
import org.camunda.bpm.model.bpmn.instance.ErrorEventDefinition
import org.camunda.bpm.model.bpmn.instance.FlowNode
import org.camunda.bpm.model.bpmn.instance.SequenceFlow
import org.camunda.bpm.model.bpmn.instance.StartEvent
import org.camunda.bpm.model.bpmn.instance.SubProcess
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnEdge
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnShape
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

@Serializable
private data class Order(var paid: Boolean) : WorkItem

@Serializable
private data class Line(var sku: String) : WorkItem

class BpmnCompilerTest {
    private fun workflowWithEverything(): Workflow<Order> = workflow("order-job") {
        input(emptyList())
        condition({ it.paid }) { task("receipt") { } } otherwise { task("dunning") { } }
        parallel {
            branch { task("index") { } }
            branch { task("notify") { } }
        }
        fanOut(expand = { emptyList<Line>() }) {
            task("price-line") { }
        }
    }

    @Test
    fun emitsOneProcessPerFanOutLevel() {
        val processes = BpmnCompiler.compile(workflowWithEverything())
        assertEquals(2, processes.size)
        assertEquals("order-job", processes[0].key)
        assertTrue(processes[1].key.startsWith("order-job_f"))
    }

    @Test
    fun rootContainsTasksDecisionExpandTopics() {
        val root = BpmnCompiler.compile(workflowWithEverything())[0]
        val topics = root.serviceTasks.map { it.topic }.toSet()
        assertTrue(topics.containsAll(setOf("receipt", "dunning", "index", "notify")))
        assertTrue(topics.any { it.startsWith("decision_") })
        assertTrue(topics.any { it.startsWith("expand_") })
    }

    @Test
    fun childLevelContainsLeafTask() {
        val child = BpmnCompiler.compile(workflowWithEverything())[1]
        assertEquals(listOf("price-line"), child.serviceTasks.map { it.topic })
    }

    @Test
    fun emitsGatewaysAndCallActivityInXml() {
        val root = BpmnCompiler.compile(workflowWithEverything())[0]
        val xml = Bpmn.convertToString(root.model)
        assertTrue("exclusiveGateway" in xml)
        assertTrue("parallelGateway" in xml)
        assertTrue("callActivity" in xml)
        assertTrue("multiInstanceLoopCharacteristics" in xml)
    }

    @Test
    fun jobTypesAreScopedToTheWorkflowAndUniqueAcrossLevels() {
        val wf = workflow<Order>("nested-job") {
            input(emptyList())
            condition({ it.paid }) { task("receipt") { } }
            fanOut(expand = { emptyList<Line>() }) {
                condition({ it.sku.isEmpty() }) { task("fix") { } }
                fanOut(expand = { emptyList<Line>() }) {
                    condition({ it.sku.isEmpty() }) { task("fix-inner") { } }
                }
            }
        }
        val refs = BpmnCompiler.compile(wf).flatMap { it.serviceTasks }
        assertTrue(refs.all { it.jobType == "nested-job/${it.topic}" })
        assertEquals(refs.size, refs.map { it.jobType }.toSet().size)
    }

    @Test
    fun everyLevelEndsTerminatedItemsThroughAnErrorEventSubprocess() {
        for (process in BpmnCompiler.compile(workflowWithEverything())) {
            val scope = process.model.getModelElementById<SubProcess>(BpmnCompiler.TERMINATE_SCOPE_ID)
            assertTrue(scope.triggeredByEvent(), process.key)
            val start = scope.getChildElementsByType(StartEvent::class.java).single()
            assertTrue(start.isInterrupting, process.key)
            val error = start.eventDefinitions.filterIsInstance<ErrorEventDefinition>().single().error
            assertEquals(EngineNames.TERMINATE_ERROR, error.errorCode)
        }
    }

    @Test
    fun timeoutExpiryPassesThroughARecordingServiceTask() {
        val wf = workflow<Order>("time-boxed") {
            input(emptyList())
            timeout(10.seconds) { task("call") { } }
        }
        val process = BpmnCompiler.compile(wf).single()
        val record = process.serviceTasks.single { it.topic.startsWith("timeout_") }
        val boundary = process.model.getModelElementsByType(BoundaryEvent::class.java).single()
        assertTrue(boundary.cancelActivity())
        assertEquals(record.id, boundary.outgoing.single().target.id)
    }

    @Test
    fun diagramInterchangeCoversEveryElementAndModelsValidate() {
        val wf = workflow<Order>("covered") {
            input(emptyList())
            condition({ it.paid }) { task("receipt") { } } otherwise { task("dunning") { } }
            loop({ !it.paid }) { task("remind") { } }
            timeout(1.seconds) { task("call") { } }
            awaitMessage("paid")
            fanOut(expand = { emptyList<Line>() }) { task("price") { } }
        }
        for (process in BpmnCompiler.compile(wf)) {
            val model = process.model
            assertEquals(
                model.getModelElementsByType(FlowNode::class.java).size,
                model.getModelElementsByType(BpmnShape::class.java).size,
                process.key,
            )
            assertEquals(
                model.getModelElementsByType(SequenceFlow::class.java).size,
                model.getModelElementsByType(BpmnEdge::class.java).size,
                process.key,
            )
            Bpmn.validateModel(model)
        }
    }
}
