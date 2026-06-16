package loadshift.camunda7

import kotlinx.serialization.Serializable
import loadshift.core.BpmnCompiler
import loadshift.core.WorkItem
import loadshift.core.workflow
import org.camunda.bpm.model.bpmn.Bpmn
import kotlin.test.Test
import kotlin.test.assertTrue

@Serializable
private data class Job(var id: String = "") : WorkItem

@Serializable
private data class Line(var sku: String = "") : WorkItem

class Camunda7DialectTest {
    @Test
    fun decoratesServiceTasksAsExternalWithTopic() {
        val wf = workflow<Job>("dialect-test") {
            input(emptyList())
            task("cleanup") { }
        }
        val process = BpmnCompiler.compile(wf).first()
        Camunda7Dialect.decorate(process.model, process.serviceTasks)
        val xml = Bpmn.convertToString(process.model)
        assertTrue("camunda:type=\"external\"" in xml, xml)
        assertTrue("camunda:topic=\"cleanup\"" in xml, xml)
    }

    @Test
    fun fanOutIteratesSpinElementsAndMapsItemIntoChild() {
        val wf = workflow<Job>("dialect-fanout") {
            input(emptyList())
            fanOut<Line>(expand = { emptyList() }) {
                task("price") { }
            }
        }
        val root = BpmnCompiler.compile(wf).first()
        Camunda7Dialect.decorate(root.model, root.serviceTasks)
        val xml = Bpmn.convertToString(root.model)
        assertTrue("camunda:collection=\"\${f1_items.elements()}\"" in xml, xml)
        assertTrue("camunda:in" in xml, xml)
        assertTrue("source=\"f1_item\"" in xml, xml)
        assertTrue("target=\"f1_item\"" in xml, xml)
    }
}
