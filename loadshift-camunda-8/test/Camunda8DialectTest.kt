package loadshift.camunda8

import kotlinx.serialization.Serializable
import loadshift.core.BpmnCompiler
import loadshift.core.EngineNames
import loadshift.core.WorkItem
import loadshift.core.fanOut
import loadshift.core.task
import loadshift.core.workflow
import org.camunda.bpm.model.bpmn.Bpmn
import org.camunda.bpm.model.bpmn.instance.CallActivity
import org.camunda.bpm.model.bpmn.instance.Message
import org.camunda.bpm.model.bpmn.instance.ServiceTask
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Serializable
private data class Job(var id: String = "") : WorkItem

@Serializable
private data class Line(var sku: String = "") : WorkItem

class Camunda8DialectTest {
    @Test
    fun injectsZeebeTaskDefinitionWithWorkflowScopedTypeAndCalledElement() {
        val wf = workflow<Job>("c8-test") {
            input(emptyList())
            task("cleanup") { }
            fanOut(expand = { emptyList<Line>() }) {
                task("price") { }
            }
        }
        val root = BpmnCompiler.compile(wf).first()
        Camunda8Dialect.decorate(root.model, root.serviceTasks)
        val xml = Bpmn.convertToString(root.model)
        assertTrue(Camunda8Dialect.ZEEBE_NS in xml, xml)
        assertTrue("type=\"c8-test/cleanup\"" in xml, xml)
        assertTrue("processId=\"c8-test_f1\"" in xml, xml)
    }

    @Test
    fun setsTaskRetriesFromTheGivenAttempts() {
        val wf = workflow<Job>("c8-retries") {
            input(emptyList())
            task("cleanup") { }
            task("archive") { }
        }
        val root = BpmnCompiler.compile(wf).first()
        Camunda8Dialect.decorate(root.model, root.serviceTasks) { ref -> if (ref.topic == "cleanup") 5 else null }

        fun retriesOf(topic: String): String? {
            val ref = root.serviceTasks.single { it.topic == topic }
            val task = root.model.getModelElementById<ServiceTask>(ref.id)
            val definition = task.extensionElements.domElement.childElements.single { it.localName == "taskDefinition" }
            return definition.getAttribute("retries")
        }
        assertEquals("5", retriesOf("cleanup"))
        assertNull(retriesOf("archive"))
    }

    @Test
    fun injectsZeebeLoopCharacteristicsAndMapsTheChildKey() {
        val wf = workflow<Job>("c8-fanout") {
            input(emptyList())
            fanOut(expand = { emptyList<Line>() }) {
                task("price") { }
            }
        }
        val root = BpmnCompiler.compile(wf).first()
        Camunda8Dialect.decorate(root.model, root.serviceTasks)
        val xml = Bpmn.convertToString(root.model)
        assertTrue("inputCollection=\"=f1_items\"" in xml, xml)
        assertTrue("inputElement=\"f1_item\"" in xml, xml)

        val call = root.model.getModelElementsByType(CallActivity::class.java).single()
        val input = call.extensionElements.domElement.childElements
            .single { it.localName == "ioMapping" }
            .childElements.single { it.localName == "input" }
        assertEquals("=f1_item.${EngineNames.ITEM_KEY}", input.getAttribute("source"))
        assertEquals(EngineNames.ITEM_KEY, input.getAttribute("target"))
    }

    @Test
    fun correlatesMessagesByRunAndItemKey() {
        val wf = workflow<Job>("c8-message") {
            input(emptyList())
            awaitMessage("go")
        }
        val root = BpmnCompiler.compile(wf).first()
        Camunda8Dialect.decorate(root.model, root.serviceTasks)
        val message = root.model.getModelElementsByType(Message::class.java).single()
        val subscription = message.extensionElements.domElement.childElements.single { it.localName == "subscription" }
        assertEquals("=${EngineNames.WORKFLOW} + \":\" + ${EngineNames.ITEM_KEY}", subscription.getAttribute("correlationKey"))
    }

    @Test
    fun rewritesTheScheduleTimerDateToFeel() {
        val schedule = BpmnCompiler.compileSchedule(
            workflow<Job>("c8-schedule") {
                input(emptyList())
                task("work") { }
            },
        )
        Camunda8Dialect.decorate(schedule.model, schedule.serviceTasks)
        val xml = Bpmn.convertToString(schedule.model)
        assertTrue(">=date and time(${EngineNames.NEXT_TICK})<" in xml, xml)
        assertTrue("type=\"c8-schedule/${EngineNames.SCHEDULE_SEED}\"" in xml, xml)
        assertTrue("\${" !in xml, xml)
    }

    @Test
    fun rewritesConditionsToFeel() {
        val wf = workflow<Job>("c8-cond") {
            input(emptyList())
            condition({ true }) {
                task("yes") { }
            } otherwise {
                task("no") { }
            }
        }
        val root = BpmnCompiler.compile(wf).first()
        Camunda8Dialect.decorate(root.model, root.serviceTasks)
        val xml = Bpmn.convertToString(root.model)
        assertTrue(">=c1_result<" in xml, xml)
        assertTrue(">=not(c1_result)<" in xml, xml)
        assertTrue("\${" !in xml, xml)
    }
}
