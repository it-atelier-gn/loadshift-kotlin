package loadshift.core

import org.camunda.bpm.model.bpmn.Bpmn
import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.builder.AbstractActivityBuilder
import org.camunda.bpm.model.bpmn.builder.AbstractFlowNodeBuilder
import org.camunda.bpm.model.bpmn.instance.Gateway

class ServiceTaskRef(val id: String, val topic: String, val jobType: String)

class CompiledProcess(
    val key: String,
    val name: String,
    val model: BpmnModelInstance,
    val serviceTasks: List<ServiceTaskRef>,
    val versionTag: String? = null,
)

object BpmnCompiler {
    const val TERMINATE_SCOPE_ID = "on_terminate"

    fun compile(workflow: Workflow<*>): List<CompiledProcess> {
        val compiled = LinkedHashMap<String, CompiledProcess>()
        for (owner in listOf(workflow) + workflow.calledWorkflows()) {
            val levels = mutableListOf<SubFlow<*>>()
            gather(owner.root, levels)
            for (level in levels) compiled.getOrPut(level.key) { LevelCompiler(owner.key, owner.version).compile(level) }
        }
        return compiled.values.toList()
    }

    fun compileSchedule(workflow: Workflow<*>): CompiledProcess {
        val key = EngineNames.scheduleProcess(workflow.key)
        val (next, seed, await) = listOf(EngineNames.SCHEDULE_NEXT, EngineNames.SCHEDULE_SEED, EngineNames.SCHEDULE_AWAIT)
            .map { topic -> ServiceTaskRef(topic, topic, EngineNames.jobType(workflow.key, topic)) }
        val model = Bpmn.createExecutableProcess(key).name(key)
            .startEvent("start").name("Start")
            .serviceTask(next.id).name("next tick")
            .intermediateCatchEvent("tick").name("tick")
            .timerWithDate("\${${EngineNames.NEXT_TICK}}")
            .serviceTask(seed.id).name("seed batch")
            .serviceTask(await.id).name("await batch")
            .connectTo(next.id)
            .done()
        BpmnLayout.apply(model, key)
        return CompiledProcess(key, key, model, listOf(next, seed, await), workflow.version)
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
            is Timeout -> walkChildren(step.body, action)
            is Execute -> step.catches.forEach { walkChildren(it.body, action) }
            is AwaitMessage -> step.onTimeout?.let { walkChildren(it, action) }
            is Wait, is AwaitSignal, is Call, is HumanTask -> Unit
        }
    }

    private class LevelCompiler(private val workflowKey: String, private val versionTag: String?) {
        private val refs = mutableListOf<ServiceTaskRef>()
        private val ids = IdGen()

        fun compile(sub: SubFlow<*>): CompiledProcess {
            val process = Bpmn.createExecutableProcess(sub.key).name(sub.key)
            val exit = step(sub.step, process.startEvent("start").name("Start"))
            exit.endEvent("end").name("End")
            process.eventSubProcess(TERMINATE_SCOPE_ID).name("on terminate")
                .startEvent("${TERMINATE_SCOPE_ID}_start").name("terminated").error(EngineNames.TERMINATE_ERROR)
                .endEvent("${TERMINATE_SCOPE_ID}_end").name("End")
            val model = process.done()
            for (gateway in model.getModelElementsByType(Gateway::class.java)) gateway.name = null
            BpmnLayout.apply(model, sub.key)
            return CompiledProcess(sub.key, sub.key, model, refs.toList(), versionTag)
        }

        private fun service(b: AbstractFlowNodeBuilder<*, *>, id: String, topic: String, label: String) =
            b.serviceTask(id).name(label).also { refs += ServiceTaskRef(id, topic, EngineNames.jobType(workflowKey, topic)) }

        private fun step(step: Step<*>, b: AbstractFlowNodeBuilder<*, *>): AbstractFlowNodeBuilder<*, *> = when (step) {
            is Sequence -> step.steps.fold(b) { current, next -> step(next, current) }

            is Execute -> {
                val taskId = "ext_${sanitizeId(step.task.topic)}_${ids.next("t")}"
                val task = service(b, taskId, step.task.topic, step.task.topic)
                if (step.catches.isEmpty()) {
                    task
                } else {
                    val join = ids.next("gw")
                    task.exclusiveGateway(join)
                    for (caught in step.catches) {
                        val boundary = b.moveToActivity<AbstractActivityBuilder<*, *>>(taskId)
                            .boundaryEvent("catch_${caught.id}")
                            .name(caught.type.simpleName ?: "error")
                            .error(EngineNames.catchError(caught.id))
                        step(caught.body, boundary).connectTo(join)
                    }
                    b.moveToNode(join)
                }
            }

            is FanOut<*, *> -> forEachChild(b, step.id, step.childKey)

            is FanIn<*, *, *> -> {
                val joined = forEachChild(b, step.id, step.childKey)
                service(joined, EngineNames.reduce(step.id), EngineNames.reduce(step.id), "reduce children")
            }

            is Conditional -> {
                val decision = EngineNames.decision(step.id)
                val split = ids.next("gw")
                val join = ids.next("gw")
                val result = EngineNames.result(step.id)
                val yes = service(b, decision, decision, "evaluate condition").exclusiveGateway(split)
                    .condition("yes", "\${$result}")
                step(step.onTrue, yes).exclusiveGateway(join)
                val no = b.moveToNode(split).condition("no", "\${!($result)}")
                (step.onFalse?.let { step(it, no) } ?: no).connectTo(join)
                b.moveToNode(join)
            }

            is Loop -> {
                val decision = EngineNames.decision(step.id)
                val split = ids.next("gw")
                val result = EngineNames.result(step.id)
                service(b, decision, decision, "loop condition").exclusiveGateway(split)
                step(step.body, b.moveToNode(split).condition("repeat", "\${$result}")).connectTo(decision)
                b.moveToNode(split).condition("done", "\${!($result)}")
            }

            is Parallel -> {
                val fork = ids.next("gw")
                val join = ids.next("gw")
                b.parallelGateway(fork)
                step.branches.forEachIndexed { index, branch ->
                    val exit = step(branch, b.moveToNode(fork))
                    if (index == 0) exit.parallelGateway(join) else exit.connectTo(join)
                }
                b.moveToNode(join)
            }

            is Wait -> b.intermediateCatchEvent("timer_${step.id}")
                .name("wait ${step.duration}")
                .timerWithDuration(step.duration.toIsoString())

            is Timeout -> {
                val scopeId = "scope_${step.id}"
                val inner = step(
                    step.body,
                    b.subProcess(scopeId).name("within ${step.duration}").embeddedSubProcess().startEvent("${scopeId}_start"),
                )
                val scope = inner.endEvent("${scopeId}_end").subProcessDone()
                val expired = scope.boundaryEvent("timeout_${step.id}")
                    .name("after ${step.duration}")
                    .cancelActivity(true)
                    .timerWithDuration(step.duration.toIsoString())
                service(expired, "record_${step.id}", EngineNames.timeout(step.id), "record timeout")
                    .endEvent("timeout_${step.id}_end")
                b.moveToNode(scopeId)
            }

            is AwaitMessage -> {
                val timeout = step.timeout
                if (timeout == null) {
                    applyMessage(step, b.intermediateCatchEvent("msg_${step.id}").name(step.message).message(step.message))
                } else {
                    val gateway = "events_${step.id}"
                    val join = ids.next("gw")
                    val received = b.eventBasedGateway().id(gateway)
                        .intermediateCatchEvent("msg_${step.id}").name(step.message).message(step.message)
                    applyMessage(step, received).exclusiveGateway(join)
                    val expired = b.moveToNode(gateway)
                        .intermediateCatchEvent("expired_${step.id}")
                        .name("after $timeout")
                        .timerWithDuration(timeout.toIsoString())
                    (step.onTimeout?.let { step(it, expired) } ?: expired).connectTo(join)
                    b.moveToNode(join)
                }
            }

            is AwaitSignal -> b.intermediateCatchEvent("sig_${step.id}")
                .name(step.signal)
                .signal(step.signal)

            is HumanTask -> {
                val waiting = b.userTask(EngineNames.userTask(step.id)).name(step.name)
                step.assignee?.let { waiting.camundaAssignee(it) }
                if (step.candidateGroups.isNotEmpty()) waiting.camundaCandidateGroups(step.candidateGroups.joinToString(","))
                service(waiting, EngineNames.form(step.id), EngineNames.form(step.id), "apply ${step.name}")
            }

            is Call -> {
                val prepare = EngineNames.call(step.id)
                val finish = EngineNames.returnCall(step.id)
                val called = service(b, prepare, prepare, "call ${step.workflow.name}")
                    .callActivity(EngineNames.callActivity(step.id))
                    .name(step.workflow.name)
                    .calledElement(step.workflow.root.key)
                service(called, finish, finish, "return from ${step.workflow.name}")
            }
        }

        private fun applyMessage(step: AwaitMessage<*>, b: AbstractFlowNodeBuilder<*, *>): AbstractFlowNodeBuilder<*, *> =
            if (step.onMessage == null) b else service(b, EngineNames.message(step.id), EngineNames.message(step.id), "apply ${step.message}")

        private fun forEachChild(b: AbstractFlowNodeBuilder<*, *>, stepId: String, childKey: String) =
            service(b, EngineNames.expand(stepId), EngineNames.expand(stepId), "expand children")
                .callActivity("call_$stepId")
                .name("for each child")
                .calledElement(childKey)
                .multiInstance()
                .parallel()
                .camundaCollection("\${${EngineNames.items(stepId)}}")
                .camundaElementVariable(EngineNames.item(stepId))
                .multiInstanceDone()
    }
}
