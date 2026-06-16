package loadshift.camunda8

import kotlinx.serialization.Serializable
import loadshift.core.BpmnCompiler
import loadshift.core.WorkItem
import loadshift.core.workflow
import org.camunda.bpm.model.bpmn.Bpmn
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Serializable
private data class Job(var id: String = "") : WorkItem

@Serializable
private data class Line(var sku: String = "") : WorkItem

class Camunda8DialectTest {
    @Test
    fun injectsZeebeTaskDefinitionAndCalledElement() {
        val wf = workflow<Job>("c8-test") {
            input(emptyList())
            task("cleanup") { }
            fanOut<Line>(expand = { emptyList() }) {
                task("price") { }
            }
        }
        val root = BpmnCompiler.compile(wf).first()
        Camunda8Dialect.decorate(root.model, root.serviceTasks)
        val xml = Bpmn.convertToString(root.model)
        assertTrue(Camunda8Dialect.ZEEBE_NS in xml, xml)
        assertTrue("taskDefinition" in xml, xml)
        assertTrue("type=\"cleanup\"" in xml, xml)
        assertTrue("calledElement" in xml, xml)
    }

    @Test
    fun injectsZeebeLoopCharacteristics() {
        val wf = workflow<Job>("c8-fanout") {
            input(emptyList())
            fanOut<Line>(expand = { emptyList() }) {
                task("price") { }
            }
        }
        val root = BpmnCompiler.compile(wf).first()
        Camunda8Dialect.decorate(root.model, root.serviceTasks)
        val xml = Bpmn.convertToString(root.model)
        assertTrue("loopCharacteristics" in xml, xml)
        assertTrue("inputCollection=\"=f1_items\"" in xml, xml)
        assertTrue("inputElement=\"f1_item\"" in xml, xml)
    }

    @Test
    fun rewritesConditionsToFeel() {
        val wf = workflow<Job>("c8-cond") {
            input(emptyList())
            condition({ true }) {
                task("yes") { }
            } otherwise {
                task("no") { }
            }
        }
        val root = BpmnCompiler.compile(wf).first()
        Camunda8Dialect.decorate(root.model, root.serviceTasks)
        val xml = Bpmn.convertToString(root.model)
        assertTrue(">=c1_result<" in xml, xml)
        assertTrue(">=not(c1_result)<" in xml, xml)
        assertTrue("\${" !in xml, xml)
    }

    @Test
    fun encodesVariablesAsPlainJson() {
        val map = mapOf("s" to "x", "i" to 7, "b" to true)
        val back = C8Variables.fromJson(C8Variables.toJson(map))
        assertEquals("x", back["s"])
        assertEquals(7, back["i"])
        assertEquals(true, back["b"])
    }
}
