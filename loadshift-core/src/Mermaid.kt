package loadshift.core

fun Workflow<*>.toMermaid(): String = MermaidWriter().render(describeFlow(this))

private class MermaidWriter {
    private val body = StringBuilder()
    private var counter = 0

    fun render(root: FlowNode): String {
        val start = round("start")
        val inner = root.children.singleOrNull()
        val end = round("end")
        if (inner == null) {
            edge(start, end)
        } else {
            val frag = node(inner)
            edge(start, frag.entry)
            edge(frag.exit, end)
        }
        return "flowchart TD\n$body"
    }

    private data class Frag(val entry: String, val exit: String)

    private fun node(n: FlowNode): Frag = when (n.type) {
        "sequence" -> sequence(n.children)
        "task" -> box(n.label).let { Frag(it, it) }
        "wait" -> round("wait").let { Frag(it, it) }
        "if" -> branch(n)
        "loop" -> loop(n)
        "parallel" -> fork(n.children)
        "fanOut" -> fan(n, "fan-out")
        "fanIn" -> fan(n, "fan-in / reduce")
        else -> box(n.type).let { Frag(it, it) }
    }

    private fun sequence(children: List<FlowNode>): Frag {
        if (children.isEmpty()) return passthrough()
        val frags = children.map { node(it) }
        for (i in 0 until frags.size - 1) edge(frags[i].exit, frags[i + 1].entry)
        return Frag(frags.first().entry, frags.last().exit)
    }

    private fun branch(n: FlowNode): Frag {
        val decision = diamond("condition")
        val merge = dot()
        val then = n.children.first { it.type == "then" }.children.single()
        val thenFrag = node(then)
        edge(decision, thenFrag.entry, "yes")
        edge(thenFrag.exit, merge)
        val otherwise = n.children.firstOrNull { it.type == "else" }?.children?.single()
        if (otherwise != null) {
            val elseFrag = node(otherwise)
            edge(decision, elseFrag.entry, "no")
            edge(elseFrag.exit, merge)
        } else {
            edge(decision, merge, "no")
        }
        return Frag(decision, merge)
    }

    private fun loop(n: FlowNode): Frag {
        val guard = diamond("guard")
        val bodyFrag = node(n.children.single())
        val exit = dot()
        edge(guard, bodyFrag.entry, "repeat")
        edge(bodyFrag.exit, guard)
        edge(guard, exit, "done")
        return Frag(guard, exit)
    }

    private fun fork(children: List<FlowNode>): Frag {
        val split = dot()
        val join = dot()
        for (branch in children) {
            val frag = node(branch)
            edge(split, frag.entry)
            edge(frag.exit, join)
        }
        return Frag(split, join)
    }

    private fun fan(n: FlowNode, label: String): Frag {
        val head = box(label)
        val join = dot()
        val child = n.children.singleOrNull()
        if (child != null) {
            val frag = node(child)
            dottedEdge(head, frag.entry, "each child")
            dottedEdge(frag.exit, join)
        } else {
            edge(head, join)
        }
        return Frag(head, join)
    }

    private fun passthrough(): Frag = dot().let { Frag(it, it) }

    private fun id(): String = "n${counter++}"
    private fun box(label: String): String = id().also { body.append("    $it[\"${esc(label)}\"]\n") }
    private fun round(label: String): String = id().also { body.append("    $it([\"${esc(label)}\"])\n") }
    private fun diamond(label: String): String = id().also { body.append("    $it{\"${esc(label)}\"}\n") }
    private fun dot(): String = id().also { body.append("    $it(( ))\n") }

    private fun edge(from: String, to: String, label: String? = null) {
        if (label == null) body.append("    $from --> $to\n") else body.append("    $from -->|${esc(label)}| $to\n")
    }

    private fun dottedEdge(from: String, to: String, label: String? = null) {
        if (label == null) body.append("    $from -.-> $to\n") else body.append("    $from -.->|${esc(label)}| $to\n")
    }

    private fun esc(s: String): String = s.replace("\"", "'")
}
