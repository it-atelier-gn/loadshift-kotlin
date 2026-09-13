package loadshift.core

import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.instance.BoundaryEvent
import org.camunda.bpm.model.bpmn.instance.FlowNode
import org.camunda.bpm.model.bpmn.instance.Process
import org.camunda.bpm.model.bpmn.instance.SequenceFlow
import org.camunda.bpm.model.bpmn.instance.SubProcess
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnEdge
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnShape
import org.camunda.bpm.model.bpmn.instance.di.Waypoint
import kotlin.math.abs
import kotlin.math.max

object BpmnLayout {
    private const val STANDARD_WIDTH = 100.0
    private const val STANDARD_HEIGHT = 80.0
    private const val COL_GAP = 70.0
    private const val ROW = 110.0
    private const val ROW_GAP = ROW - STANDARD_HEIGHT
    private const val MARGIN = 60.0
    private const val LOOP_GAP = 45.0
    private const val BOUNDARY_ANCHOR = 0.75

    fun apply(model: BpmnModelInstance, processId: String) {
        val process = model.getModelElementById(processId) as? Process ?: return
        val children = process.getChildElementsByType(FlowNode::class.java).toList()
        val eventScopes = children.filterIsInstance<SubProcess>().filter { it.triggeredByEvent() }
        val boundaries = children.filterIsInstance<BoundaryEvent>()
        val nodes = children.filterNot { it in eventScopes || it in boundaries }
        if (nodes.isEmpty()) return
        val ids = nodes.map { it.id }
        val idSet = ids.toSet()
        val boundaryIds = boundaries.map { it.id }.toSet()

        val shapes = model.getModelElementsByType(BpmnShape::class.java)
            .filter { it.bpmnElement != null }
            .associateBy { it.bpmnElement.id }
        val edges = model.getModelElementsByType(BpmnEdge::class.java)
            .filter { it.bpmnElement != null }
            .associateBy { it.bpmnElement.id }

        val detours = boundaries.flatMap { event ->
            val host = event.attachedTo?.id ?: return@flatMap emptyList()
            event.outgoing.mapNotNull { it.target?.id }.map { host to it }
        }
        val out = ids.associateWith { id ->
            model.node(id).outgoing.mapNotNull { it.target?.id }.filter { it in idSet } +
                detours.filter { it.first == id }.map { it.second }
        }
        val inc = ids.associateWith { id ->
            model.node(id).incoming.mapNotNull { it.source?.id }.filter { it in idSet } +
                detours.filter { it.second == id }.map { it.first }
        }

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

        val backSources = backEdges.map { it.first }.toSet()
        val sinks = ids.filter { fOut.getValue(it).isEmpty() && it !in backSources }.toHashSet()
        val canReach = HashSet(sinks)
        var grew = true
        while (grew) {
            grew = false
            for (u in ids) if (u !in canReach && fOut.getValue(u).any { it in canReach }) {
                canReach.add(u)
                grew = true
            }
        }
        val satellites = ids.filterNot { it in canReach }.toHashSet()

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

        fun w(id: String) = shapes[id]?.bounds?.width ?: 100.0
        fun h(id: String) = shapes[id]?.bounds?.height ?: 80.0

        fun gap(above: String, below: String) = h(above) / 2 + h(below) / 2 + ROW_GAP

        val cx = HashMap<String, Double>()
        val cy = HashMap<String, Double>()
        var left = MARGIN
        for (r in rankList) {
            val lst = layers.getValue(r)
            val width = max(STANDARD_WIDTH, lst.maxOf { w(it) })
            for (id in lst) cx[id] = left + width / 2
            left += width + COL_GAP
        }
        for ((_, lst) in layers) {
            val k = lst.size
            lst.forEachIndexed { i, id -> cy[id] = (i - (k - 1) / 2.0) * ROW }
        }
        repeat(8) { sweep ->
            val downward = sweep % 2 == 0
            fun desired(id: String): Double {
                val neigh = (if (downward) inc.getValue(id).filter { forward(it, id) }
                else out.getValue(id).filter { forward(id, it) }).filter { it !in satellites }
                val ys = neigh.mapNotNull { cy[it] }
                return if (ys.isEmpty()) cy.getValue(id) else ys.average()
            }
            for (r in if (downward) rankList else rankList.reversed()) {
                val lst = layers.getValue(r)
                val mains = lst.filter { it !in satellites }
                if (mains.isNotEmpty()) {
                    val want = mains.map { desired(it) }
                    val ys = DoubleArray(mains.size)
                    for (i in mains.indices) {
                        ys[i] = if (i == 0) want[i] else max(want[i], ys[i - 1] + gap(mains[i - 1], mains[i]))
                    }
                    val shift = want.average() - ys.average()
                    for (i in mains.indices) cy[mains[i]] = ys[i] + shift
                }
                var previous = mains.maxByOrNull { cy.getValue(it) }
                for (t in lst.filter { it in satellites }) {
                    val want = inc.getValue(t).filter { forward(it, t) }.mapNotNull { cy[it] }
                    val target = if (want.isEmpty()) cy.getValue(t) else want.average()
                    val y = previous?.let { max(target, cy.getValue(it) + gap(it, t)) } ?: target
                    cy[t] = y
                    previous = t
                }
            }
        }

        val dx = MARGIN - ids.minOf { cx.getValue(it) - w(it) / 2 }
        val dy = MARGIN - ids.minOf { cy.getValue(it) - h(it) / 2 }
        for (id in ids) {
            cx[id] = cx.getValue(id) + dx
            cy[id] = cy.getValue(id) + dy
        }

        for (id in ids) {
            val bounds = shapes[id]?.bounds ?: continue
            val x = cx.getValue(id) - w(id) / 2
            val y = cy.getValue(id) - h(id) / 2
            val node = model.node(id)
            if (node is SubProcess) translateContents(node, x - bounds.x, y - bounds.y, shapes, edges)
            bounds.x = x
            bounds.y = y
        }

        for (boundary in boundaries) {
            val host = boundary.attachedTo?.id?.let { shapes[it]?.bounds } ?: continue
            val bounds = shapes[boundary.id]?.bounds ?: continue
            cx[boundary.id] = host.x + host.width * BOUNDARY_ANCHOR
            cy[boundary.id] = host.y + host.height
            bounds.x = cx.getValue(boundary.id) - bounds.width / 2
            bounds.y = cy.getValue(boundary.id) - bounds.height / 2
        }

        val bottom = cx.keys.maxOf { cy.getValue(it) + h(it) / 2 } + LOOP_GAP
        for (edge in edges.values) {
            val flow = edge.bpmnElement as? SequenceFlow ?: continue
            val u = flow.source?.id ?: continue
            val v = flow.target?.id ?: continue
            if (cx[u] == null || cx[v] == null) continue
            val ucx = cx.getValue(u); val ucy = cy.getValue(u)
            val vcx = cx.getValue(v); val vcy = cy.getValue(v)
            val pts: List<Pair<Double, Double>> = if ((u to v) in backEdges) {
                listOf(ucx to ucy + h(u) / 2, ucx to bottom, vcx to bottom, vcx to vcy + h(v) / 2)
            } else if (u in boundaryIds) {
                val sy = ucy + h(u) / 2
                val tx = vcx - w(v) / 2
                if (vcy >= sy) {
                    listOf(ucx to sy, ucx to vcy, tx to vcy)
                } else {
                    val drop = sy + LOOP_GAP / 3
                    val mx = (ucx + tx) / 2
                    listOf(ucx to sy, ucx to drop, mx to drop, mx to vcy, tx to vcy)
                }
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

        var top = bottom + ROW / 2
        for (scope in eventScopes) {
            val bounds = shapes[scope.id]?.bounds ?: continue
            translateContents(scope, MARGIN - bounds.x, top - bounds.y, shapes, edges)
            bounds.x = MARGIN
            bounds.y = top
            top += bounds.height + ROW / 2
        }
    }

    private fun translateContents(
        container: SubProcess,
        dx: Double,
        dy: Double,
        shapes: Map<String, BpmnShape>,
        edges: Map<String, BpmnEdge>,
    ) {
        if (dx == 0.0 && dy == 0.0) return
        for (node in container.getChildElementsByType(FlowNode::class.java)) {
            shapes[node.id]?.bounds?.let {
                it.x += dx
                it.y += dy
            }
            if (node is SubProcess) translateContents(node, dx, dy, shapes, edges)
        }
        for (flow in container.getChildElementsByType(SequenceFlow::class.java)) {
            edges[flow.id]?.waypoints?.forEach {
                it.x += dx
                it.y += dy
            }
        }
    }

    private fun BpmnModelInstance.node(id: String): FlowNode =
        getModelElementById(id) as FlowNode
}
