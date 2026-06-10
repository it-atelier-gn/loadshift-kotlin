package loadshift.core

import org.camunda.bpm.model.bpmn.Bpmn
import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.builder.AbstractFlowNodeBuilder

class ServiceTaskRef(val id: String, val topic: String)

class CompiledProcess(
    val key: String,
    val name: String,
    val model: BpmnModelInstance,
    val serviceTasks: List<ServiceTaskRef>,
)

object BpmnCompiler {

    fun compile(workflow: Workflow<*>): List<CompiledProcess> {
        val levels = mutableListOf<SubFlow<*>>()
        gather(workflow.root, levels)
        return levels.map { compileLevel(it) }
    }

    private fun gather(sub: SubFlow<*>, acc: MutableList<SubFlow<*>>) {
        acc += sub
        walkFanOuts(sub.step) { fanOut -> gather(fanOut.body, acc) }
    }

    private fun walkFanOuts(step: Step<*>, action: (FanOut<*, *>) -> Unit) {
        when (step) {
            is Sequence -> step.steps.forEach { walkFanOuts(it, action) }
            is Conditional -> {
                walkFanOuts(step.onTrue, action)
                step.onFalse?.let { walkFanOuts(it, action) }
            }
            is Loop -> walkFanOuts(step.body, action)
            is Parallel -> step.branches.forEach { walkFanOuts(it, action) }
            is FanOut<*, *> -> action(step)
            is Execute -> {}
        }
    }

    private fun compileLevel(sub: SubFlow<*>): CompiledProcess {
        val refs = mutableListOf<ServiceTaskRef>()
        val gateways = IdGen()

        val process = Bpmn.createExecutableProcess(sub.key).name(sub.key)
        var builder: AbstractFlowNodeBuilder<*, *> = process.startEvent("start").name("Start")

        builder = compileStep(sub.step, builder, refs, gateways)
        val model = builder.endEvent("end").name("End").done()
        return CompiledProcess(sub.key, sub.key, model, refs)
    }

    private fun compileStep(
        step: Step<*>,
        b: AbstractFlowNodeBuilder<*, *>,
        refs: MutableList<ServiceTaskRef>,
        gw: IdGen,
    ): AbstractFlowNodeBuilder<*, *> {
        return when (step) {
            is Sequence -> {
                var cur = b
                for (s in step.steps) cur = compileStep(s, cur, refs, gw)
                cur
            }

            is Execute -> {
                val id = "ext_${sanitizeId(step.task.topic)}_${gw.next("t")}"
                refs += ServiceTaskRef(id, step.task.topic)
                b.serviceTask(id).name(step.task.topic)
            }

            is FanOut<*, *> -> {
                val expandId = "expand_${step.id}"
                refs += ServiceTaskRef(expandId, "expand_${step.id}")
                val collection = "${step.id}_items"
                val element = "${step.id}_item"
                b.serviceTask(expandId).name(expandId)
                    .callActivity("call_${step.id}")
                    .calledElement(step.childKey)
                    .multiInstance()
                    .parallel()
                    .camundaCollection("\${$collection}")
                    .camundaElementVariable(element)
                    .multiInstanceDone()
            }

            is Conditional -> {
                val decisionId = "decision_${step.id}"
                refs += ServiceTaskRef(decisionId, "decision_${step.id}")
                val split = gw.next("gw")
                val join = gw.next("gw")
                val resultExpr = "\${${step.id}_result}"

                var trueB: AbstractFlowNodeBuilder<*, *> = b.serviceTask(decisionId).name(decisionId)
                    .exclusiveGateway(split)
                    .condition("true", resultExpr)
                trueB = compileStep(step.onTrue, trueB, refs, gw)
                trueB.exclusiveGateway(join)

                val falseB = b.moveToNode(split).condition("false", "\${!(${step.id}_result)}")
                val falseExit = step.onFalse?.let { compileStep(it, falseB, refs, gw) } ?: falseB
                falseExit.connectTo(join)

                b.moveToNode(join)
            }

            is Loop -> {
                val decisionId = "decision_${step.id}"
                refs += ServiceTaskRef(decisionId, "decision_${step.id}")
                val split = gw.next("gw")
                val exit = gw.next("gw")
                val resultExpr = "\${${step.id}_result}"

                b.serviceTask(decisionId).name(decisionId).exclusiveGateway(split)
                var bodyB = b.moveToNode(split).condition("loop", resultExpr)
                bodyB = compileStep(step.body, bodyB, refs, gw)
                bodyB.connectTo(decisionId)

                b.moveToNode(split).condition("exit", "\${!(${step.id}_result)}").exclusiveGateway(exit)
            }

            is Parallel -> {
                val fork = gw.next("gw")
                val join = gw.next("gw")
                b.parallelGateway(fork)
                step.branches.forEachIndexed { index, branch ->
                    val branchB = b.moveToNode(fork)
                    val exit = compileStep(branch, branchB, refs, gw)
                    if (index == 0) exit.parallelGateway(join) else exit.connectTo(join)
                }
                b.moveToNode(join)
            }
        }
    }
}
