package loadshift.camunda8

import loadshift.core.EngineNames
import loadshift.core.ServiceTaskRef
import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.instance.BaseElement
import org.camunda.bpm.model.bpmn.instance.CallActivity
import org.camunda.bpm.model.bpmn.instance.ConditionExpression
import org.camunda.bpm.model.bpmn.instance.ExtensionElements
import org.camunda.bpm.model.bpmn.instance.Message
import org.camunda.bpm.model.bpmn.instance.MultiInstanceLoopCharacteristics
import org.camunda.bpm.model.bpmn.instance.Process
import org.camunda.bpm.model.bpmn.instance.ServiceTask
import org.camunda.bpm.model.bpmn.instance.TimeDate
import org.camunda.bpm.model.bpmn.instance.UserTask

object Camunda8Dialect {
    const val ZEEBE_NS = "http://camunda.org/schema/zeebe/1.0"
    private const val CAMUNDA_NS = "http://camunda.org/schema/1.0/bpmn"

    fun decorate(
        model: BpmnModelInstance,
        serviceTasks: List<ServiceTaskRef>,
        versionTag: String? = null,
        maxAttempts: (ServiceTaskRef) -> Int? = { null },
    ) {
        if (versionTag != null) {
            for (process in model.getModelElementsByType(Process::class.java)) {
                ensureExtensions(model, process).addExtensionElement(ZEEBE_NS, "versionTag").domElement.setAttribute("value", versionTag)
            }
        }
        for (ref in serviceTasks) {
            val task = model.getModelElementById(ref.id) as? ServiceTask ?: continue
            val definition = ensureExtensions(model, task).addExtensionElement(ZEEBE_NS, "taskDefinition")
            definition.domElement.setAttribute("type", ref.jobType)
            maxAttempts(ref)?.let { definition.domElement.setAttribute("retries", it.toString()) }
        }
        for (call in model.getModelElementsByType(CallActivity::class.java)) {
            val childId = call.calledElement ?: continue
            val extensions = ensureExtensions(model, call)
            val called = extensions.addExtensionElement(ZEEBE_NS, "calledElement")
            called.domElement.setAttribute("processId", childId)
            called.domElement.setAttribute("propagateAllChildVariables", "false")
            val mi = call.loopCharacteristics as? MultiInstanceLoopCharacteristics
            if (mi == null) {
                called.domElement.setAttribute("propagateAllParentVariables", "false")
                val stepId = call.id.removePrefix(EngineNames.callActivity(""))
                val mapping = extensions.addExtensionElement(ZEEBE_NS, "ioMapping")
                fun map(kind: String, source: String, target: String) {
                    val entry = model.document.createElement(ZEEBE_NS, kind)
                    entry.setAttribute("source", source)
                    entry.setAttribute("target", target)
                    mapping.domElement.appendChild(entry)
                }
                map("input", "=${EngineNames.callItem(stepId)}", EngineNames.CALL_ITEM)
                map("input", "=\"$childId\"", EngineNames.WORKFLOW)
                map("input", "=${EngineNames.ITEM_KEY}", EngineNames.ITEM_KEY)
                map("output", "=${EngineNames.CALL_ITEM}", EngineNames.callItem(stepId))
                continue
            }
            val element = mi.camundaElementVariable ?: continue
            val mapping = extensions.addExtensionElement(ZEEBE_NS, "ioMapping")
            val input = model.document.createElement(ZEEBE_NS, "input")
            input.setAttribute("source", "=$element.${EngineNames.ITEM_KEY}")
            input.setAttribute("target", EngineNames.ITEM_KEY)
            mapping.domElement.appendChild(input)
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
            subscription.domElement.setAttribute(
                "correlationKey",
                "=${EngineNames.WORKFLOW} + \":\" + ${EngineNames.ITEM_KEY}",
            )
        }
        for (task in model.getModelElementsByType(UserTask::class.java)) {
            val extensions = ensureExtensions(model, task)
            extensions.addExtensionElement(ZEEBE_NS, "userTask")
            val assignee = task.camundaAssignee
            val groups = task.camundaCandidateGroups
            if (assignee != null || groups != null) {
                val assignment = extensions.addExtensionElement(ZEEBE_NS, "assignmentDefinition")
                assignee?.let { assignment.domElement.setAttribute("assignee", it) }
                groups?.let { assignment.domElement.setAttribute("candidateGroups", it) }
            }
            task.domElement.removeAttribute(CAMUNDA_NS, "assignee")
            task.domElement.removeAttribute(CAMUNDA_NS, "candidateGroups")
        }
        for (condition in model.getModelElementsByType(ConditionExpression::class.java)) {
            condition.textContent = toFeel(condition.textContent.trim())
        }
        for (date in model.getModelElementsByType(TimeDate::class.java)) {
            val text = date.textContent.trim()
            if (text.startsWith("\${") && text.endsWith("}")) {
                date.textContent = "=date and time(${text.removeSurrounding("\${", "}")})"
            }
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
