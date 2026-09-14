package loadshift.camunda7

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
import loadshift.core.UserTask
import loadshift.core.WorkItem
import loadshift.core.workflow
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Serializable
private data class Ticket(var id: String = "") : WorkItem

class Camunda7UserTaskTest {
    private val jsonHeaders = headersOf(HttpHeaders.ContentType, "application/json")

    private val approvals = workflow<Ticket>("c7-approvals") {
        input(emptyList())
        userTask("approve", candidateGroups = listOf("reviewers")) { _, _ -> }
    }

    @Test
    fun openUserTasksAreListedWithTheirItemKeysAndCompletedWithTheForm() = runTest {
        val completions = mutableListOf<JsonObject>()
        val engine = MockEngine { request ->
            when (request.url.encodedPath) {
                "/engine-rest/task" -> {
                    assertEquals("c7-approvals", request.url.parameters["processDefinitionKey"])
                    respond(
                        """[{"id":"t-1","name":"approve","taskDefinitionKey":"user_ut1","processInstanceId":"pi-1","processDefinitionId":"4f2c-uuid"},""" +
                            """{"id":"t-2","name":"other","taskDefinitionKey":"user_other","processInstanceId":"pi-2","processDefinitionId":"4f2c-uuid"}]""",
                        HttpStatusCode.OK,
                        jsonHeaders,
                    )
                }
                "/engine-rest/variable-instance" -> respond(
                    """[{"processInstanceId":"pi-1","value":"a","type":"String"}]""",
                    HttpStatusCode.OK,
                    jsonHeaders,
                )
                "/engine-rest/task/t-1" -> respond(
                    """{"id":"t-1","taskDefinitionKey":"user_ut1","processInstanceId":"pi-1"}""",
                    HttpStatusCode.OK,
                    jsonHeaders,
                )
                "/engine-rest/task/t-9" -> respond("""{"message":"not found"}""", HttpStatusCode.NotFound, jsonHeaders)
                "/engine-rest/task/t-1/complete" -> {
                    completions += Json.parseToJsonElement((request.body as TextContent).text).jsonObject
                    respond("", HttpStatusCode.NoContent)
                }
                else -> error("unexpected request ${request.url}")
            }
        }
        val backend = Camunda7Backend(client = Camunda7Client("http://engine/engine-rest", null, engine))

        assertEquals(listOf(UserTask("t-1", "c7-approvals", "approve", "a", null, listOf("reviewers"))), backend.userTasks(approvals))
        assertTrue(backend.completeUserTask("t-1", JsonObject(mapOf("decision" to JsonPrimitive("yes")))))
        assertFalse(backend.completeUserTask("t-9", JsonObject(emptyMap())))

        val form = completions.single().getValue("variables").jsonObject.getValue("ut1_form").jsonObject
        assertEquals("json", form.getValue("type").jsonPrimitive.content)
        assertEquals("""{"decision":"yes"}""", form.getValue("value").jsonPrimitive.content)
    }
}
