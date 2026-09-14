package loadshift.camunda8

import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.TextContent
import io.ktor.http.headersOf
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import loadshift.core.BpmnCompiler
import loadshift.core.UserTask
import loadshift.core.WorkItem
import loadshift.core.workflow
import org.camunda.bpm.model.bpmn.Bpmn
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Serializable
private data class Ticket(var id: String = "") : WorkItem

class Camunda8UserTaskTest {
    private val jsonHeaders = headersOf(HttpHeaders.ContentType, "application/json")

    private val approvals = workflow<Ticket>("c8-approvals") {
        input(emptyList())
        userTask("approve", assignee = "ops", candidateGroups = listOf("reviewers", "leads")) { _, _ -> }
    }

    @Test
    fun userTasksBecomeCamundaUserTasksWithAnAssignmentDefinition() {
        val root = BpmnCompiler.compile(approvals).single()

        Camunda8Dialect.decorate(root.model, root.serviceTasks)

        val task = root.model.getModelElementsByType(org.camunda.bpm.model.bpmn.instance.UserTask::class.java).single()
        val extensions = task.extensionElements.domElement.childElements
        assertTrue(extensions.any { it.localName == "userTask" && it.namespaceURI == Camunda8Dialect.ZEEBE_NS })
        val assignment = extensions.single { it.localName == "assignmentDefinition" }
        assertEquals("ops", assignment.getAttribute("assignee"))
        assertEquals("reviewers,leads", assignment.getAttribute("candidateGroups"))
        assertEquals(null, task.camundaAssignee)
        assertEquals(null, task.camundaCandidateGroups)
        Bpmn.validateModel(root.model)
    }

    @Test
    fun openUserTasksAreListedWithTheirItemKeysAndCompletedWithTheForm() = runTest {
        val completions = mutableListOf<JsonObject>()
        val engine = MockEngine { request ->
            when (request.url.encodedPath) {
                "/v2/user-tasks/search" -> {
                    val filter = Json.parseToJsonElement((request.body as TextContent).text).jsonObject.getValue("filter").jsonObject
                    assertEquals("CREATED", filter.getValue("state").jsonPrimitive.content)
                    respond(
                        """{"items":[{"userTaskKey":"5","name":"approve","state":"CREATED","assignee":"ops","elementId":"user_ut1",""" +
                            """"processDefinitionId":"c8-approvals","processInstanceKey":"10","candidateGroups":["reviewers","leads"]}],"page":{}}""",
                        HttpStatusCode.OK,
                        jsonHeaders,
                    )
                }
                "/v2/variables/search" -> respond("""{"items":[{"processInstanceKey":"10","value":"\"a\""}]}""", HttpStatusCode.OK, jsonHeaders)
                "/v2/user-tasks/5" -> respond(
                    """{"userTaskKey":"5","state":"CREATED","elementId":"user_ut1","processInstanceKey":"10"}""",
                    HttpStatusCode.OK,
                    jsonHeaders,
                )
                "/v2/user-tasks/6" -> respond(
                    """{"userTaskKey":"6","state":"COMPLETED","elementId":"user_ut1","processInstanceKey":"10"}""",
                    HttpStatusCode.OK,
                    jsonHeaders,
                )
                "/v2/user-tasks/9" -> respond("", HttpStatusCode.NotFound)
                "/v2/user-tasks/5/completion" -> {
                    completions += Json.parseToJsonElement((request.body as TextContent).text).jsonObject
                    respond("", HttpStatusCode.NoContent)
                }
                else -> error("unexpected request ${request.url}")
            }
        }
        val backend = Camunda8Backend(client = Camunda8Client("http://engine", Camunda8Auth.None, engine))

        assertEquals(
            listOf(UserTask("5", "c8-approvals", "approve", "a", "ops", listOf("reviewers", "leads"))),
            backend.userTasks(approvals),
        )
        assertTrue(backend.completeUserTask("5", JsonObject(mapOf("decision" to JsonPrimitive("yes")))))
        assertFalse(backend.completeUserTask("6", JsonObject(emptyMap())))
        assertFalse(backend.completeUserTask("9", JsonObject(emptyMap())))

        val form = completions.single().getValue("variables").jsonObject.getValue("ut1_form").jsonObject
        assertEquals("yes", form.getValue("decision").jsonPrimitive.content)
    }
}
