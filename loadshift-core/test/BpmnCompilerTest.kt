package loadshift.core

import kotlinx.serialization.Serializable
import org.camunda.bpm.model.bpmn.Bpmn
import org.camunda.bpm.model.bpmn.instance.BoundaryEvent
import org.camunda.bpm.model.bpmn.instance.CallActivity
import org.camunda.bpm.model.bpmn.instance.ErrorEventDefinition
import org.camunda.bpm.model.bpmn.instance.EventBasedGateway
import org.camunda.bpm.model.bpmn.instance.ExclusiveGateway
import org.camunda.bpm.model.bpmn.instance.IntermediateCatchEvent
import org.camunda.bpm.model.bpmn.instance.MessageEventDefinition
import org.camunda.bpm.model.bpmn.instance.ServiceTask
import org.camunda.bpm.model.bpmn.instance.TimerEventDefinition
import org.camunda.bpm.model.bpmn.instance.FlowNode
import org.camunda.bpm.model.bpmn.instance.SequenceFlow
import org.camunda.bpm.model.bpmn.instance.SignalEventDefinition
import org.camunda.bpm.model.bpmn.instance.StartEvent
import org.camunda.bpm.model.bpmn.instance.SubProcess
import org.camunda.bpm.model.bpmn.instance.TimeDate
import org.camunda.bpm.model.bpmn.instance.UserTask
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnEdge
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnShape
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

@Serializable
private data class Order(var paid: Boolean) : WorkItem

@Serializable
private data class Line(var sku: String) : WorkItem

private class OutOfStock : RuntimeException("out of stock")

class BpmnCompilerTest {
    private fun workflowWithEverything(): Workflow<Order> = workflow("order-job") {
        input(emptyList())
        condition({ it.paid }) { task("receipt") { } } otherwise { task("dunning") { } }
        parallel {
            branch { task("index") { } }
            branch { task("notify") { } }
        }
        fanOut(expand = { emptyList<Line>() }) {
            task("price-line") { }
        }
    }

    @Test
    fun emitsOneProcessPerFanOutLevel() {
        val processes = BpmnCompiler.compile(workflowWithEverything())
        assertEquals(2, processes.size)
        assertEquals("order-job", processes[0].key)
        assertTrue(processes[1].key.startsWith("order-job_f"))
    }

    @Test
    fun rootContainsTasksDecisionExpandTopics() {
        val root = BpmnCompiler.compile(workflowWithEverything())[0]
        val topics = root.serviceTasks.map { it.topic }.toSet()
        assertTrue(topics.containsAll(setOf("receipt", "dunning", "index", "notify")))
        assertTrue(topics.any { it.startsWith("decision_") })
        assertTrue(topics.any { it.startsWith("expand_") })
    }

    @Test
    fun scheduleProcessLoopsFromTheTickTimerThroughSeedAndAwait() {
        val schedule = BpmnCompiler.compileSchedule(workflowWithEverything())
        val next = EngineNames.SCHEDULE_NEXT
        val seed = EngineNames.SCHEDULE_SEED
        val await = EngineNames.SCHEDULE_AWAIT

        assertEquals("order-job_loadshift_schedule", schedule.key)
        assertEquals(listOf("order-job/$next", "order-job/$seed", "order-job/$await"), schedule.serviceTasks.map { it.jobType })
        val flows = schedule.model.getModelElementsByType(SequenceFlow::class.java).map { it.source.id to it.target.id }.toSet()
        assertEquals(setOf("start" to next, next to "tick", "tick" to seed, seed to await, await to next), flows)
        assertEquals("\${${EngineNames.NEXT_TICK}}", schedule.model.getModelElementsByType(TimeDate::class.java).single().textContent)
        assertEquals(5, schedule.model.getModelElementsByType(BpmnShape::class.java).size)
    }

    @Test
    fun userTaskCompilesToAUserTaskWithAssignmentFollowedByTheFormTask() {
        val wf = workflow<Order>("approval-job") {
            input(emptyList())
            userTask("Approve order", assignee = "ops", candidateGroups = listOf("reviewers", "leads")) { _, _ -> }
        }

        val process = BpmnCompiler.compile(wf).single()

        val task = process.model.getModelElementById<UserTask>("user_ut1")
        assertEquals("Approve order", task.name)
        assertEquals("ops", task.camundaAssignee)
        assertEquals("reviewers,leads", task.camundaCandidateGroups)
        assertEquals(listOf("approval-job/form_ut1"), process.serviceTasks.map { it.jobType })
        assertEquals(listOf("Approve order"), wf.humanTasks().map { it.step.name })
        kotlin.test.assertFailsWith<IllegalArgumentException> {
            workflow<Order>("bad-groups") { userTask("x", candidateGroups = listOf("a,b")) }
        }
    }

    @Test
    fun theWorkflowVersionIsTheVersionTagOfEveryProcessItOwns() {
        val billing = workflow<Order>("billing-versioned") {
            version("7")
            input(emptyList())
            task("invoice") { }
        }
        val checkout = workflow<Order>("checkout-versioned") {
            version("3.1")
            input(emptyList())
            fanOut(expand = { emptyList<Line>() }) { task("price") { } }
            call(billing)
        }

        val tags = BpmnCompiler.compile(checkout).associate { it.key to it.versionTag }

        assertEquals(mapOf("checkout-versioned" to "3.1", "checkout-versioned_f1" to "3.1", "billing-versioned" to "7"), tags)
        assertEquals("3.1", BpmnCompiler.compileSchedule(checkout).versionTag)
        kotlin.test.assertFailsWith<IllegalArgumentException> {
            workflow<Order>("blank-version") { version(" ") }
        }
    }

    @Test
    fun callCompilesToACallActivityAndIncludesTheCalledWorkflow() {
        val billing = workflow<Order>("billing") {
            input(emptyList())
            task("invoice") { }
        }
        val checkout = workflow<Order>("checkout") {
            input(emptyList())
            task("reserve") { }
            call(billing)
            task("ship") { }
        }

        val processes = BpmnCompiler.compile(checkout)

        assertEquals(listOf("checkout", "billing"), processes.map { it.key })
        val activity = processes[0].model.getModelElementsByType(CallActivity::class.java).single()
        assertEquals("billing", activity.calledElement)
        assertEquals(
            listOf("checkout/reserve", "checkout/call_cw1", "checkout/return_cw1", "checkout/ship"),
            processes[0].serviceTasks.map { it.jobType },
        )
        assertEquals(listOf("billing/invoice"), processes[1].serviceTasks.map { it.jobType })
        assertEquals(listOf("billing"), checkout.calledWorkflows().map { it.key })
    }

    @Test
    fun awaitSignalCompilesToASignalCatchEvent() {
        val wf = workflow<Order>("signal-job") {
            input(emptyList())
            awaitSignal("stock-arrived")
            task("ship") { }
        }
        val model = BpmnCompiler.compile(wf).single().model

        val definition = model.getModelElementsByType(SignalEventDefinition::class.java).single()
        assertEquals("stock-arrived", definition.signal.name)
        assertEquals(listOf("ship"), BpmnCompiler.compile(wf).single().serviceTasks.map { it.topic })
    }

    @Test
    fun childLevelContainsLeafTask() {
        val child = BpmnCompiler.compile(workflowWithEverything())[1]
        assertEquals(listOf("price-line"), child.serviceTasks.map { it.topic })
    }

    @Test
    fun emitsGatewaysAndCallActivityInXml() {
        val root = BpmnCompiler.compile(workflowWithEverything())[0]
        val xml = Bpmn.convertToString(root.model)
        assertTrue("exclusiveGateway" in xml)
        assertTrue("parallelGateway" in xml)
        assertTrue("callActivity" in xml)
        assertTrue("multiInstanceLoopCharacteristics" in xml)
    }

    @Test
    fun jobTypesAreScopedToTheWorkflowAndUniqueAcrossLevels() {
        val wf = workflow<Order>("nested-job") {
            input(emptyList())
            condition({ it.paid }) { task("receipt") { } }
            fanOut(expand = { emptyList<Line>() }) {
                condition({ it.sku.isEmpty() }) { task("fix") { } }
                fanOut(expand = { emptyList<Line>() }) {
                    condition({ it.sku.isEmpty() }) { task("fix-inner") { } }
                }
            }
        }
        val refs = BpmnCompiler.compile(wf).flatMap { it.serviceTasks }
        assertTrue(refs.all { it.jobType == "nested-job/${it.topic}" })
        assertEquals(refs.size, refs.map { it.jobType }.toSet().size)
    }

    @Test
    fun everyLevelEndsTerminatedItemsThroughAnErrorEventSubprocess() {
        for (process in BpmnCompiler.compile(workflowWithEverything())) {
            val scope = process.model.getModelElementById<SubProcess>(BpmnCompiler.TERMINATE_SCOPE_ID)
            assertTrue(scope.triggeredByEvent(), process.key)
            val start = scope.getChildElementsByType(StartEvent::class.java).single()
            assertTrue(start.isInterrupting, process.key)
            val error = start.eventDefinitions.filterIsInstance<ErrorEventDefinition>().single().error
            assertEquals(EngineNames.TERMINATE_ERROR, error.errorCode)
        }
    }

    @Test
    fun timeoutExpiryPassesThroughARecordingServiceTask() {
        val wf = workflow<Order>("time-boxed") {
            input(emptyList())
            timeout(10.seconds) { task("call") { } }
        }
        val process = BpmnCompiler.compile(wf).single()
        val record = process.serviceTasks.single { it.topic.startsWith("timeout_") }
        val boundary = process.model.getModelElementsByType(BoundaryEvent::class.java).single()
        assertTrue(boundary.cancelActivity())
        assertEquals(record.id, boundary.outgoing.single().target.id)
    }

    @Test
    fun diagramInterchangeCoversEveryElementAndModelsValidate() {
        val wf = workflow<Order>("covered") {
            input(emptyList())
            condition({ it.paid }) { task("receipt") { } } otherwise { task("dunning") { } }
            loop({ !it.paid }) { task("remind") { } }
            timeout(1.seconds) { task("call") { } }
            awaitMessage("paid")
            awaitMessage("shipped", timeout = 5.seconds) { _, _ -> } onTimeout { task("chase") { } }
            task("reserve") { }.catching<OutOfStock> { task("backorder") { } }
            fanOut(expand = { emptyList<Line>() }) { task("price") { } }
        }
        for (process in BpmnCompiler.compile(wf)) {
            val model = process.model
            assertEquals(
                model.getModelElementsByType(FlowNode::class.java).size,
                model.getModelElementsByType(BpmnShape::class.java).size,
                process.key,
            )
            assertEquals(
                model.getModelElementsByType(SequenceFlow::class.java).size,
                model.getModelElementsByType(BpmnEdge::class.java).size,
                process.key,
            )
            Bpmn.validateModel(model)
        }
    }

    @Test
    fun caughtErrorsLeaveTheTaskThroughAnErrorBoundaryEventAndRejoin() {
        val wf = workflow<Order>("catching-job") {
            input(emptyList())
            task("reserve") { }.catching<OutOfStock> { task("backorder") { } }
            task("ship") { }
        }
        val process = BpmnCompiler.compile(wf).single()
        val model = process.model

        val boundary = model.getModelElementsByType(BoundaryEvent::class.java).single()
        assertEquals("catch_ce1", boundary.id)
        assertEquals("OutOfStock", boundary.name)
        assertTrue(boundary.cancelActivity())
        assertEquals(process.serviceTasks.single { it.topic == "reserve" }.id, boundary.attachedTo.id)
        val code = boundary.eventDefinitions.filterIsInstance<ErrorEventDefinition>().single().error.errorCode
        assertEquals(EngineNames.catchError("ce1"), code)
        val backorder = model.getModelElementById<ServiceTask>(process.serviceTasks.single { it.topic == "backorder" }.id)
        assertEquals(backorder.id, boundary.outgoing.single().target.id)
        val join = backorder.outgoing.single().target as ExclusiveGateway
        val reserve = model.getModelElementById<ServiceTask>(boundary.attachedTo.id)
        assertEquals(join.id, reserve.outgoing.single().target.id)
        assertEquals(process.serviceTasks.single { it.topic == "ship" }.id, join.outgoing.single().target.id)
    }

    @Test
    fun messageDataIsAppliedByAServiceTaskAfterTheCatchEvent() {
        val wf = workflow<Order>("message-data-job") {
            input(emptyList())
            awaitMessage("paid") { order, data -> order.paid = data.isNotEmpty() }
            awaitMessage("noted")
        }
        val process = BpmnCompiler.compile(wf).single()

        assertEquals(listOf(EngineNames.message("msg1")), process.serviceTasks.map { it.topic })
        val event = process.model.getModelElementById<IntermediateCatchEvent>("msg_msg1")
        assertEquals(EngineNames.message("msg1"), event.outgoing.single().target.id)
        assertTrue(process.model.getModelElementsByType(EventBasedGateway::class.java).isEmpty())
    }

    @Test
    fun messageWithTimeoutRacesTheMessageAgainstATimerThroughAnEventBasedGateway() {
        val wf = workflow<Order>("message-timeout-job") {
            input(emptyList())
            awaitMessage("paid", timeout = 30.seconds) { _, _ -> } onTimeout { task("remind") { } }
            task("ship") { }
        }
        val process = BpmnCompiler.compile(wf).single()
        val model = process.model

        val gateway = model.getModelElementsByType(EventBasedGateway::class.java).single()
        val targets = gateway.outgoing.map { it.target as IntermediateCatchEvent }.associateBy { it.id }
        assertEquals(setOf("msg_msg1", "expired_msg1"), targets.keys)
        assertEquals(1, targets.getValue("msg_msg1").eventDefinitions.filterIsInstance<MessageEventDefinition>().size)
        val timer = targets.getValue("expired_msg1").eventDefinitions.filterIsInstance<TimerEventDefinition>().single()
        assertEquals("PT30S", timer.timeDuration.textContent)
        val remind = process.serviceTasks.single { it.topic == "remind" }.id
        assertEquals(remind, targets.getValue("expired_msg1").outgoing.single().target.id)
        val apply = model.getModelElementById<ServiceTask>(EngineNames.message("msg1"))
        val join = apply.outgoing.single().target as ExclusiveGateway
        assertEquals(join.id, model.getModelElementById<ServiceTask>(remind).outgoing.single().target.id)
        assertEquals(process.serviceTasks.single { it.topic == "ship" }.id, join.outgoing.single().target.id)
    }

    @Test
    fun branchesOfCatchesAndTimeoutsReachFanOutsInsideThem() {
        val wf = workflow<Order>("nested-branches") {
            input(emptyList())
            task("reserve") { }.catching<OutOfStock> {
                fanOut(expand = { emptyList<Line>() }) { task("split") { } }
            }
            awaitMessage("paid", timeout = 1.seconds) onTimeout {
                fanOut(expand = { emptyList<Line>() }) { task("remind-line") { } }
            }
        }
        val processes = BpmnCompiler.compile(wf).associateBy { it.key }

        assertEquals(setOf("nested-branches", "nested-branches_f2", "nested-branches_f4"), processes.keys)
        assertEquals(listOf("split"), processes.getValue("nested-branches_f2").serviceTasks.map { it.topic })
        assertEquals(setOf("nested-branches_f2", "nested-branches_f4"), wf.levels().keys - "nested-branches")
    }
}
