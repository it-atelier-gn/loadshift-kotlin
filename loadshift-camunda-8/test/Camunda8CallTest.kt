package loadshift.camunda8

import kotlinx.serialization.Serializable
import loadshift.core.BpmnCompiler
import loadshift.core.EngineNames
import loadshift.core.WorkItem
import loadshift.core.task
import loadshift.core.workflow
import org.camunda.bpm.model.bpmn.instance.CallActivity
import kotlin.test.Test
import kotlin.test.assertEquals

@Serializable
private data class CallParcel(var id: String = "") : WorkItem

class Camunda8CallTest {

    @Test
    fun callActivitiesMapThePackedItemInAndOutWithoutPropagatingOtherVariables() {
        val billing = workflow<CallParcel>("c8-billing") {
            input(emptyList())
            task("invoice") { }
        }
        val checkout = workflow<CallParcel>("c8-checkout") {
            input(emptyList())
            call(billing)
        }
        val root = BpmnCompiler.compile(checkout).first()

        Camunda8Dialect.decorate(root.model, root.serviceTasks)

        val call = root.model.getModelElementsByType(CallActivity::class.java).single()
        val extensions = call.extensionElements.domElement.childElements
        val called = extensions.single { it.localName == "calledElement" }
        assertEquals("c8-billing", called.getAttribute("processId"))
        assertEquals("false", called.getAttribute("propagateAllParentVariables"))
        assertEquals("false", called.getAttribute("propagateAllChildVariables"))
        val mappings = extensions.single { it.localName == "ioMapping" }.childElements
            .map { Triple(it.localName, it.getAttribute("source"), it.getAttribute("target")) }
        assertEquals(
            listOf(
                Triple("input", "=cw1_call", EngineNames.CALL_ITEM),
                Triple("input", "=\"c8-billing\"", EngineNames.WORKFLOW),
                Triple("input", "=${EngineNames.ITEM_KEY}", EngineNames.ITEM_KEY),
                Triple("output", "=${EngineNames.CALL_ITEM}", "cw1_call"),
            ),
            mappings,
        )
    }
}
