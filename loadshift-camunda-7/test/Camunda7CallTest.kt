package loadshift.camunda7

import kotlinx.serialization.Serializable
import loadshift.core.BpmnCompiler
import loadshift.core.EngineNames
import loadshift.core.WorkItem
import loadshift.core.task
import loadshift.core.workflow
import org.camunda.bpm.model.bpmn.instance.CallActivity
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaIn
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaOut
import kotlin.test.Test
import kotlin.test.assertEquals

@Serializable
private data class CallParcel(var id: String = "") : WorkItem

class Camunda7CallTest {

    @Test
    fun callActivitiesPassThePackedItemTheItemKeyAndTheCalledWorkflowKey() {
        val billing = workflow<CallParcel>("c7-billing") {
            input(emptyList())
            task("invoice") { }
        }
        val checkout = workflow<CallParcel>("c7-checkout") {
            input(emptyList())
            call(billing)
        }
        val root = BpmnCompiler.compile(checkout).first()

        Camunda7Dialect.decorate(root.model, root.serviceTasks)

        val call = root.model.getModelElementsByType(CallActivity::class.java).single()
        val ins = call.extensionElements.elementsQuery.filterByType(CamundaIn::class.java).list()
        val outs = call.extensionElements.elementsQuery.filterByType(CamundaOut::class.java).list()
        assertEquals(
            setOf("cw1_call" to EngineNames.CALL_ITEM, EngineNames.ITEM_KEY to EngineNames.ITEM_KEY),
            ins.filter { it.camundaSource != null }.map { it.camundaSource to it.camundaTarget }.toSet(),
        )
        assertEquals("\${'c7-billing'}", ins.single { it.camundaTarget == EngineNames.WORKFLOW }.camundaSourceExpression)
        assertEquals(listOf(EngineNames.CALL_ITEM to "cw1_call"), outs.map { it.camundaSource to it.camundaTarget })
    }
}
