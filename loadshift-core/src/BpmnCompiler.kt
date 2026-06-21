package loadshift.core

import org.camunda.bpm.model.bpmn.Bpmn
import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.builder.AbstractFlowNodeBuilder
import org.camunda.bpm.model.bpmn.instance.Gateway

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
        walkChildren(sub.step) { child -> gather(child, acc) }
    }

    private fun walkChildren(step: Step<*>, action: (SubFlow<*>) -> Unit) {
        when (step) {
            is Sequence -> step.steps.forEach { walkChildren(it, action) }
            is Conditional -> {
                walkChildren(step.onTrue, action)
                step.onFalse?.let { walkChildren(it, action) }
            }
            is Loop -> walkChildren(step.body, action)
            is Parallel -> step.branches.forEach { walkChildren(it, action) }
            is FanOut<*, *> -> action(step.body)
            is FanIn<*, *, *> -> action(step.body)
            is Execute -> {}
            is Wait -> {}
            is Timeout -> walkChildren(step.body, action)
            is AwaitMessage -> {}
        }
    }

    private fun compileLevel(sub: SubFlow<*>): CompiledProcess {
        val refs = mutableListOf<ServiceTaskRef>()
        val gateways = IdGen()

        val process = Bpmn.createExecutableProcess(sub.key).name(sub.key)
        var builder: AbstractFlowNodeBuilder<*, *> = process.startEvent("start").name("Start")

        builder = compileStep(sub.step, builder, refs, gateways)
        val model = builder.endEvent("end").name("End").done()
        for (gateway in model.getModelElementsByType(Gateway::class.java)) gateway.name = null
        BpmnLayout.apply(model, sub.key)
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
                b.serviceTask(expandId).name("expand children")
                    .callActivity("call_${step.id}")
                    .name("for each child")
                    .calledElement(step.childKey)
                    .multiInstance()
                    .parallel()
                    .camundaCollection("\${$collection}")
                    .camundaElementVariable(element)
                    .multiInstanceDone()
            }

            is FanIn<*, *, *> -> {
                val expandId = "expand_${step.id}"
                refs += ServiceTaskRef(expandId, "expand_${step.id}")
                val reduceId = "reduce_${step.id}"
                val collection = "${step.id}_items"
                val element = "${step.id}_item"
                val withChildren = b.serviceTask(expandId).name("expand children")
                    .callActivity("call_${step.id}")
                    .name("for each child")
                    .calledElement(step.childKey)
                    .multiInstance()
                    .parallel()
                    .camundaCollection("\${$collection}")
                    .camundaElementVariable(element)
                    .multiInstanceDone()
                refs += ServiceTaskRef(reduceId, "reduce_${step.id}")
                withChildren.serviceTask(reduceId).name("reduce children")
            }

            is Conditional -> {
                val decisionId = "decision_${step.id}"
                refs += ServiceTaskRef(decisionId, "decision_${step.id}")
                val split = gw.next("gw")
                val join = gw.next("gw")
                val resultExpr = "\${${step.id}_result}"

                var trueB: AbstractFlowNodeBuilder<*, *> = b.serviceTask(decisionId).name("evaluate condition")
                    .exclusiveGateway(split)
                    .condition("yes", resultExpr)
                trueB = compileStep(step.onTrue, trueB, refs, gw)
                trueB.exclusiveGateway(join)

                val falseB = b.moveToNode(split).condition("no", "\${!(${step.id}_result)}")
                val falseExit = step.onFalse?.let { compileStep(it, falseB, refs, gw) } ?: falseB
                falseExit.connectTo(join)

                b.moveToNode(join)
            }

            is Loop -> {
                val decisionId = "decision_${step.id}"
                refs += ServiceTaskRef(decisionId, "decision_${step.id}")
                val split = gw.next("gw")
                val resultExpr = "\${${step.id}_result}"

                b.serviceTask(decisionId).name("loop condition").exclusiveGateway(split)
                var bodyB = b.moveToNode(split).condition("repeat", resultExpr)
                bodyB = compileStep(step.body, bodyB, refs, gw)
                bodyB.connectTo(decisionId)

                b.moveToNode(split).condition("done", "\${!(${step.id}_result)}")
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

            is Wait -> b.intermediateCatchEvent("timer_${step.id}")
                .name("wait ${step.duration}")
                .timerWithDuration(step.duration.toIsoString())

            is Timeout -> {
                val scopeId = "scope_${step.id}"
                var inner: AbstractFlowNodeBuilder<*, *> = b.subProcess(scopeId)
                    .name("within ${step.duration}")
                    .embeddedSubProcess()
                    .startEvent("${scopeId}_start")
                inner = compileStep(step.body, inner, refs, gw)
                val scope = inner.endEvent("${scopeId}_end").subProcessDone()
                scope.boundaryEvent("timeout_${step.id}")
                    .cancelActivity(true)
                    .timerWithDuration(step.duration.toIsoString())
                    .endEvent("timeout_${step.id}_end")
                b.moveToNode(scopeId)
            }

            is AwaitMessage -> b.intermediateCatchEvent("msg_${step.id}")
                .name(step.message)
                .message(step.message)
        }
    }
}
