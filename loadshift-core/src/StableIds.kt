package loadshift.core

import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.instance.SequenceFlow
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnEdge
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnShape
import org.camunda.bpm.model.xml.instance.DomElement
import org.camunda.bpm.model.xml.instance.ModelElementInstance
import org.camunda.bpm.model.xml.type.attribute.Attribute

object StableIds {
    private val GENERATED = Regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

    fun apply(model: BpmnModelInstance) {
        val taken = mutableSetOf<String>()
        val generated = mutableListOf<ModelElementInstance>()
        fun visit(dom: DomElement) {
            val id = dom.getAttribute("id")
            if (id != null) {
                if (GENERATED.containsMatchIn(id)) {
                    model.getModelElementById<ModelElementInstance>(id)?.let { generated += it }
                } else {
                    taken += id
                }
            }
            dom.childElements.forEach(::visit)
        }
        visit(model.document.rootElement)
        val (diagram, semantic) = generated.partition { it is BpmnShape || it is BpmnEdge }
        val counters = mutableMapOf<String, Int>()
        for (element in semantic + diagram) {
            val base = when (element) {
                is SequenceFlow -> "flow_${element.source?.id}_${element.target?.id}"
                is BpmnShape -> "${element.bpmnElement?.id}_di"
                is BpmnEdge -> "${element.bpmnElement?.id}_di"
                else -> element.elementType.typeName
            }
            val numbered = element !is SequenceFlow && element !is BpmnShape && element !is BpmnEdge
            var candidate: String
            do {
                val n = counters.merge(base, 1, Int::plus)!!
                candidate = if (!numbered && n == 1) base else "${base}_$n"
            } while (candidate in taken)
            taken += candidate
            @Suppress("UNCHECKED_CAST")
            (element.elementType.getAttribute("id") as Attribute<String>).setValue(element, candidate)
        }
    }
}
