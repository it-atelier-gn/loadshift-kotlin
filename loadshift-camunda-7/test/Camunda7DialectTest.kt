package loadshift.camunda7

import loadshift.core.BpmnCompiler
import loadshift.core.WorkItemBase
import loadshift.core.workflow
import org.camunda.bpm.model.bpmn.Bpmn
import kotlin.test.Test
import kotlin.test.assertTrue

private class Job : WorkItemBase() {
    var id: String by required()
}

class Camunda7DialectTest {
    @Test
    fun decoratesServiceTasksAsExternalWithTopic() {
        val wf = workflow<Job>("dialect-test") {
            items(emptyList())
            task("cleanup") { }
        }
        val process = BpmnCompiler.compile(wf).first()
        Camunda7Dialect.decorate(process.model, process.serviceTasks)
        val xml = Bpmn.convertToString(process.model)
        assertTrue("camunda:type=\"external\"" in xml, xml)
        assertTrue("camunda:topic=\"cleanup\"" in xml, xml)
    }
}
