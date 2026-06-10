package loadshift.camunda8

import loadshift.core.ServiceTaskRef
import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.instance.BaseElement
import org.camunda.bpm.model.bpmn.instance.CallActivity
import org.camunda.bpm.model.bpmn.instance.ExtensionElements
import org.camunda.bpm.model.bpmn.instance.ServiceTask

object Camunda8Dialect {
    const val ZEEBE_NS = "http://camunda.org/schema/zeebe/1.0"

    fun decorate(model: BpmnModelInstance, serviceTasks: List<ServiceTaskRef>) {
        for (ref in serviceTasks) {
            val task = model.getModelElementById(ref.id) as? ServiceTask ?: continue
            val definition = ensureExtensions(model, task).addExtensionElement(ZEEBE_NS, "taskDefinition")
            definition.domElement.setAttribute("type", ref.topic)
        }
        for (call in model.getModelElementsByType(CallActivity::class.java)) {
            val childId = call.calledElement ?: continue
            val called = ensureExtensions(model, call).addExtensionElement(ZEEBE_NS, "calledElement")
            called.domElement.setAttribute("processId", childId)
            called.domElement.setAttribute("propagateAllChildVariables", "false")
        }
    }

    private fun ensureExtensions(model: BpmnModelInstance, element: BaseElement): ExtensionElements {
        element.extensionElements?.let { return it }
        val extensions = model.newInstance(ExtensionElements::class.java)
        element.extensionElements = extensions
        return extensions
    }
}
