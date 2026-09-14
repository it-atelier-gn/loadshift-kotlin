package loadshift.camunda7

import loadshift.core.EngineNames
import loadshift.core.ServiceTaskRef
import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.instance.BaseElement
import org.camunda.bpm.model.bpmn.instance.CallActivity
import org.camunda.bpm.model.bpmn.instance.ExtensionElements
import org.camunda.bpm.model.bpmn.instance.MultiInstanceLoopCharacteristics
import org.camunda.bpm.model.bpmn.instance.Process
import org.camunda.bpm.model.bpmn.instance.ServiceTask
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaIn
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaOut

object Camunda7Dialect {
    fun decorate(model: BpmnModelInstance, serviceTasks: List<ServiceTaskRef>, versionTag: String? = null) {
        if (versionTag != null) {
            for (process in model.getModelElementsByType(Process::class.java)) process.camundaVersionTag = versionTag
        }
        for (ref in serviceTasks) {
            val task = model.getModelElementById(ref.id) as? ServiceTask ?: continue
            task.camundaType = "external"
            task.camundaTopic = ref.jobType
        }
        for (call in model.getModelElementsByType(CallActivity::class.java)) {
            val mi = call.loopCharacteristics as? MultiInstanceLoopCharacteristics
            if (mi == null) {
                mapCall(model, call)
                continue
            }
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

    private fun mapCall(model: BpmnModelInstance, call: CallActivity) {
        val stepId = call.id.removePrefix(EngineNames.callActivity(""))
        val extensions = ensureExtensions(model, call)
        extensions.addChildElement(
            model.newInstance(CamundaIn::class.java).apply {
                camundaSource = EngineNames.callItem(stepId)
                camundaTarget = EngineNames.CALL_ITEM
            },
        )
        extensions.addChildElement(
            model.newInstance(CamundaIn::class.java).apply {
                camundaSourceExpression = "\${'${call.calledElement}'}"
                camundaTarget = EngineNames.WORKFLOW
            },
        )
        extensions.addChildElement(
            model.newInstance(CamundaIn::class.java).apply {
                camundaSource = EngineNames.ITEM_KEY
                camundaTarget = EngineNames.ITEM_KEY
            },
        )
        extensions.addChildElement(
            model.newInstance(CamundaOut::class.java).apply {
                camundaSource = EngineNames.CALL_ITEM
                camundaTarget = EngineNames.callItem(stepId)
            },
        )
    }

    private fun ensureExtensions(model: BpmnModelInstance, element: BaseElement): ExtensionElements {
        element.extensionElements?.let { return it }
        val extensions = model.newInstance(ExtensionElements::class.java)
        element.extensionElements = extensions
        return extensions
    }
}
