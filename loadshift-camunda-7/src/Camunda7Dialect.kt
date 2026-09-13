package loadshift.camunda7

import loadshift.core.EngineNames
import loadshift.core.ServiceTaskRef
import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.instance.BaseElement
import org.camunda.bpm.model.bpmn.instance.CallActivity
import org.camunda.bpm.model.bpmn.instance.ExtensionElements
import org.camunda.bpm.model.bpmn.instance.MultiInstanceLoopCharacteristics
import org.camunda.bpm.model.bpmn.instance.ServiceTask
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaIn

object Camunda7Dialect {
    fun decorate(model: BpmnModelInstance, serviceTasks: List<ServiceTaskRef>) {
        for (ref in serviceTasks) {
            val task = model.getModelElementById(ref.id) as? ServiceTask ?: continue
            task.camundaType = "external"
            task.camundaTopic = ref.jobType
        }
        for (call in model.getModelElementsByType(CallActivity::class.java)) {
            val mi = call.loopCharacteristics as? MultiInstanceLoopCharacteristics ?: continue
            mi.camundaCollection?.let { collection ->
                mi.camundaCollection = collection.removeSuffix("}") + ".elements()}"
            }
            val element = mi.camundaElementVariable ?: continue
            val extensions = ensureExtensions(model, call)
            extensions.addChildElement(
                model.newInstance(CamundaIn::class.java).apply {
                    camundaSource = element
                    camundaTarget = element
                },
            )
            extensions.addChildElement(
                model.newInstance(CamundaIn::class.java).apply {
                    camundaSource = EngineNames.WORKFLOW
                    camundaTarget = EngineNames.WORKFLOW
                },
            )
            extensions.addChildElement(
                model.newInstance(CamundaIn::class.java).apply {
                    camundaSourceExpression = "\${$element.prop('${EngineNames.ITEM_KEY}').stringValue()}"
                    camundaTarget = EngineNames.ITEM_KEY
                },
            )
        }
    }

    private fun ensureExtensions(model: BpmnModelInstance, element: BaseElement): ExtensionElements {
        element.extensionElements?.let { return it }
        val extensions = model.newInstance(ExtensionElements::class.java)
        element.extensionElements = extensions
        return extensions
    }
}
