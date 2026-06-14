package loadshift.core

import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.instance.FlowNode
import org.camunda.bpm.model.bpmn.instance.Process
import org.camunda.bpm.model.bpmn.instance.SequenceFlow
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnEdge
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnShape
import org.camunda.bpm.model.bpmn.instance.di.Waypoint
import kotlin.math.abs
import kotlin.math.max

object BpmnLayout {
    private const val COL = 170.0
    private const val ROW = 110.0
    private const val MARGIN = 60.0
    private const val LOOP_GAP = 45.0

    fun apply(model: BpmnModelInstance, processId: String) {
        val process = model.getModelElementById(processId) as? Process ?: return
        val nodes = process.getChildElementsByType(FlowNode::class.java).toList()
        if (nodes.isEmpty()) return
        val ids = nodes.map { it.id }

        val out = ids.associateWith { id -> model.node(id).outgoing.mapNotNull { it.target?.id } }
        val inc = ids.associateWith { id -> model.node(id).incoming.mapNotNull { it.source?.id } }

        val start = nodes.firstOrNull { inc.getValue(it.id).isEmpty() }?.id ?: ids.first()

        val backEdges = HashSet<Pair<String, String>>()
        val color = HashMap<String, Int>()
        val discovery = ArrayList<String>()
        fun visit(u: String) {
            color[u] = 1
            discovery.add(u)
            for (v in out.getValue(u)) when (color[v]) {
                null -> visit(v)
                1 -> backEdges.add(u to v)
                else -> {}
            }
            color[u] = 2
        }
        visit(start)
        for (id in ids) if (color[id] == null) visit(id)

        fun forward(u: String, v: String) = (u to v) !in backEdges

        val fOut = ids.associateWith { u -> out.getValue(u).filter { forward(u, it) } }
        val indeg = HashMap<String, Int>().apply { ids.forEach { this[it] = 0 } }
        for (u in ids) for (v in fOut.getValue(u)) indeg[v] = indeg.getValue(v) + 1
        val rank = HashMap<String, Int>().apply { ids.forEach { this[it] = 0 } }
        val queue = ArrayDeque(ids.filter { indeg.getValue(it) == 0 })
        while (queue.isNotEmpty()) {
            val u = queue.removeFirst()
            for (v in fOut.getValue(u)) {
                rank[v] = max(rank.getValue(v), rank.getValue(u) + 1)
                indeg[v] = indeg.getValue(v) - 1
                if (indeg.getValue(v) == 0) queue.addLast(v)
            }
        }

        val layers = sortedMapOf<Int, MutableList<String>>()
        for (id in discovery) layers.getOrPut(rank.getValue(id)) { mutableListOf() }.add(id)
        val rankList = layers.keys.toList()

        fun positions(): HashMap<String, Double> {
            val p = HashMap<String, Double>()
            for ((_, lst) in layers) lst.forEachIndexed { i, id -> p[id] = i.toDouble() }
            return p
        }
        repeat(4) { sweep ->
            val p = positions()
            val seq = if (sweep % 2 == 0) rankList.drop(1) else rankList.dropLast(1).reversed()
            for (r in seq) layers.getValue(r).sortBy { id ->
                val neigh = if (sweep % 2 == 0) inc.getValue(id).filter { forward(it, id) }
                else out.getValue(id).filter { forward(id, it) }
                val xs = neigh.mapNotNull { p[it] }
                if (xs.isEmpty()) p.getValue(id) else xs.average()
            }
        }

        val shapes = model.getModelElementsByType(BpmnShape::class.java)
            .filter { it.bpmnElement != null }
            .associateBy { it.bpmnElement.id }
        fun w(id: String) = shapes[id]?.bounds?.width ?: 100.0
        fun h(id: String) = shapes[id]?.bounds?.height ?: 80.0

        val cx = HashMap<String, Double>()
        val cy = HashMap<String, Double>()
        for ((r, lst) in layers) for (id in lst) cx[id] = MARGIN + r * COL
        for ((_, lst) in layers) {
            val k = lst.size
            lst.forEachIndexed { i, id -> cy[id] = (i - (k - 1) / 2.0) * ROW }
        }
        repeat(8) { sweep ->
            val seq = if (sweep % 2 == 0) rankList else rankList.reversed()
            for (r in seq) {
                val lst = layers.getValue(r)
                val desired = lst.map { id ->
                    val neigh = if (sweep % 2 == 0) inc.getValue(id).filter { forward(it, id) }
                    else out.getValue(id).filter { forward(id, it) }
                    val ys = neigh.mapNotNull { cy[it] }
                    if (ys.isEmpty()) cy.getValue(id) else ys.average()
                }
                val ys = DoubleArray(lst.size)
                for (i in lst.indices) ys[i] = if (i == 0) desired[i] else max(desired[i], ys[i - 1] + ROW)
                val shift = desired.average() - ys.average()
                for (i in lst.indices) cy[lst[i]] = ys[i] + shift
            }
        }

        val dx = MARGIN - ids.minOf { cx.getValue(it) - w(it) / 2 }
        val dy = MARGIN - ids.minOf { cy.getValue(it) - h(it) / 2 }
        for (id in ids) {
            cx[id] = cx.getValue(id) + dx
            cy[id] = cy.getValue(id) + dy
        }

        for (id in ids) {
            val b = shapes[id]?.bounds ?: continue
            b.x = cx.getValue(id) - w(id) / 2
            b.y = cy.getValue(id) - h(id) / 2
        }

        val bottom = ids.maxOf { cy.getValue(it) + h(it) / 2 } + LOOP_GAP
        for (edge in model.getModelElementsByType(BpmnEdge::class.java)) {
            val flow = edge.bpmnElement as? SequenceFlow ?: continue
            val u = flow.source?.id ?: continue
            val v = flow.target?.id ?: continue
            if (cx[u] == null || cx[v] == null) continue
            val ucx = cx.getValue(u); val ucy = cy.getValue(u)
            val vcx = cx.getValue(v); val vcy = cy.getValue(v)
            val pts: List<Pair<Double, Double>> = if ((u to v) in backEdges) {
                listOf(ucx to ucy + h(u) / 2, ucx to bottom, vcx to bottom, vcx to vcy + h(v) / 2)
            } else {
                val sx = ucx + w(u) / 2
                val tx = vcx - w(v) / 2
                if (abs(ucy - vcy) < 1.0) {
                    listOf(sx to ucy, tx to vcy)
                } else {
                    val mx = (sx + tx) / 2
                    listOf(sx to ucy, mx to ucy, mx to vcy, tx to vcy)
                }
            }
            val wps = edge.waypoints
            ArrayList(wps).forEach { wps.remove(it) }
            for ((px, py) in pts) {
                wps.add(model.newInstance(Waypoint::class.java).apply { x = px; y = py })
            }
        }
    }

    private fun BpmnModelInstance.node(id: String): FlowNode =
        getModelElementById(id) as FlowNode
}
