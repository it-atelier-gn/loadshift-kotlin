package loadshift.camunda7

import kotlinx.serialization.Serializable
import loadshift.core.BpmnCompiler
import loadshift.core.EngineNames
import loadshift.core.WorkItem
import loadshift.core.fanOut
import loadshift.core.task
import loadshift.core.workflow
import org.camunda.bpm.model.bpmn.Bpmn
import org.camunda.bpm.model.bpmn.instance.CallActivity
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaIn
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Serializable
private data class Job(var id: String = "") : WorkItem

@Serializable
private data class Line(var sku: String = "") : WorkItem

class Camunda7DialectTest {
    @Test
    fun decoratesServiceTasksAsExternalTasksWithWorkflowScopedTopics() {
        val wf = workflow<Job>("dialect-test") {
            input(emptyList())
            task("cleanup") { }
        }
        val process = BpmnCompiler.compile(wf).first()
        Camunda7Dialect.decorate(process.model, process.serviceTasks)
        val xml = Bpmn.convertToString(process.model)
        assertTrue("camunda:type=\"external\"" in xml, xml)
        assertTrue("camunda:topic=\"dialect-test/cleanup\"" in xml, xml)
    }

    @Test
    fun fanOutIteratesSpinElementsAndMapsItemRunAndKeyIntoChild() {
        val wf = workflow<Job>("dialect-fanout") {
            input(emptyList())
            fanOut(expand = { emptyList<Line>() }) {
                task("price") { }
            }
        }
        val root = BpmnCompiler.compile(wf).first()
        Camunda7Dialect.decorate(root.model, root.serviceTasks)
        val xml = Bpmn.convertToString(root.model)
        assertTrue("camunda:collection=\"\${f1_items.elements()}\"" in xml, xml)

        val call = root.model.getModelElementsByType(CallActivity::class.java).single()
        val inputs = call.extensionElements.elementsQuery.filterByType(CamundaIn::class.java).list()
        assertEquals(setOf("f1_item", EngineNames.WORKFLOW, EngineNames.ITEM_KEY), inputs.map { it.camundaTarget }.toSet())
        assertEquals("f1_item", inputs.single { it.camundaTarget == "f1_item" }.camundaSource)
        assertEquals(EngineNames.WORKFLOW, inputs.single { it.camundaTarget == EngineNames.WORKFLOW }.camundaSource)
        assertEquals(
            "\${f1_item.prop('${EngineNames.ITEM_KEY}').stringValue()}",
            inputs.single { it.camundaTarget == EngineNames.ITEM_KEY }.camundaSourceExpression,
        )
    }
}
