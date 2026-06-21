package loadshift.camunda8

import loadshift.core.ServiceTaskRef
import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.instance.BaseElement
import org.camunda.bpm.model.bpmn.instance.CallActivity
import org.camunda.bpm.model.bpmn.instance.ConditionExpression
import org.camunda.bpm.model.bpmn.instance.ExtensionElements
import org.camunda.bpm.model.bpmn.instance.Message
import org.camunda.bpm.model.bpmn.instance.MultiInstanceLoopCharacteristics
import org.camunda.bpm.model.bpmn.instance.ServiceTask

object Camunda8Dialect {
    const val ZEEBE_NS = "http://camunda.org/schema/zeebe/1.0"
    private const val CAMUNDA_NS = "http://camunda.org/schema/1.0/bpmn"

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
        for (mi in model.getModelElementsByType(MultiInstanceLoopCharacteristics::class.java)) {
            val collection = mi.camundaCollection?.removeSurrounding("\${", "}") ?: continue
            val loop = ensureExtensions(model, mi).addExtensionElement(ZEEBE_NS, "loopCharacteristics")
            loop.domElement.setAttribute("inputCollection", "=$collection")
            mi.camundaElementVariable?.let { loop.domElement.setAttribute("inputElement", it) }
            mi.domElement.removeAttribute(CAMUNDA_NS, "collection")
            mi.domElement.removeAttribute(CAMUNDA_NS, "elementVariable")
        }
        for (message in model.getModelElementsByType(Message::class.java)) {
            val subscription = ensureExtensions(model, message).addExtensionElement(ZEEBE_NS, "subscription")
            subscription.domElement.setAttribute("correlationKey", "=_ls_key")
        }
        for (condition in model.getModelElementsByType(ConditionExpression::class.java)) {
            condition.textContent = toFeel(condition.textContent.trim())
        }
    }

    private fun toFeel(juel: String): String {
        val inner = juel.removeSurrounding("\${", "}")
        val feel = if (inner.startsWith("!(") && inner.endsWith(")")) {
            "not(${inner.substring(2, inner.length - 1)})"
        } else {
            inner
        }
        return "=$feel"
    }

    private fun ensureExtensions(model: BpmnModelInstance, element: BaseElement): ExtensionElements {
        element.extensionElements?.let { return it }
        val extensions = model.newInstance(ExtensionElements::class.java)
        element.extensionElements = extensions
        return extensions
    }
}
