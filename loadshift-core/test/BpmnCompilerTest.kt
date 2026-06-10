package loadshift.core

import org.camunda.bpm.model.bpmn.Bpmn
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private class Order(vars: MutableMap<String, Any?> = mutableMapOf()) : WorkItemBase(vars) {
    var paid: Boolean by required(variables)
}

private class Line(vars: MutableMap<String, Any?> = mutableMapOf()) : WorkItemBase(vars) {
    var sku: String by required(variables)
}

class BpmnCompilerTest {
    private fun workflowWithEverything(): Workflow<Order> = workflow("order-job") {
        items(emptyList())
        ifThen({ it.paid }) { task("receipt") { } } elseThen { task("dunning") { } }
        parallel {
            branch { task("index") { } }
            branch { task("notify") { } }
        }
        forEach<Line>(expand = { emptyList() }) {
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
}
