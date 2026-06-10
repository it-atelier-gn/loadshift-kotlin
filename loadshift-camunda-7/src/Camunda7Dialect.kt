package loadshift.camunda7

import loadshift.core.ServiceTaskRef
import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.instance.ServiceTask

object Camunda7Dialect {
    fun decorate(model: BpmnModelInstance, serviceTasks: List<ServiceTaskRef>) {
        for (ref in serviceTasks) {
            val task = model.getModelElementById(ref.id) as? ServiceTask ?: continue
            task.camundaType = "external"
            task.camundaTopic = ref.topic
        }
    }
}
