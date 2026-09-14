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
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import loadshift.core.BpmnCompiler
import loadshift.core.MigrationFailure
import loadshift.core.MigrationResult
import loadshift.core.WorkItem
import loadshift.core.task
import loadshift.core.workflow
import org.camunda.bpm.model.bpmn.instance.Process
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Serializable
private data class Shipment(var id: String = "") : WorkItem

class Camunda7MigrationTest {
    private val jsonHeaders = headersOf(HttpHeaders.ContentType, "application/json")

    private fun body(content: Any): JsonObject = Json.parseToJsonElement((content as TextContent).text).jsonObject

    private val versioned = workflow<Shipment>("c7-versioned") {
        version("2")
        input(emptyList())
        task("ship") { }
    }

    @Test
    fun versionTagIsSetOnEveryProcess() {
        val root = BpmnCompiler.compile(versioned).single()

        Camunda7Dialect.decorate(root.model, root.serviceTasks, root.versionTag)

        assertEquals(listOf("2"), root.model.getModelElementsByType(Process::class.java).map { it.camundaVersionTag })
    }

    @Test
    fun migrateMovesInstancesOfOlderDefinitionsToTheLatestDefinition() = runTest {
        val executed = mutableListOf<JsonObject>()
        val engine = MockEngine { request ->
            when (request.url.encodedPath) {
                "/engine-rest/deployment/create" -> respond("""{"id":"d-2"}""", HttpStatusCode.OK, jsonHeaders)
                "/engine-rest/process-definition" -> {
                    assertEquals("true", request.url.parameters["latestVersion"])
                    respond("""[{"id":"def-2","key":"c7-versioned","version":2,"versionTag":"2"}]""", HttpStatusCode.OK, jsonHeaders)
                }
                "/engine-rest/process-instance" -> respond(
                    """[{"id":"pi-1","definitionId":"def-1"},{"id":"pi-2","definitionId":"def-2"},{"id":"pi-3","definitionId":"def-0"}]""",
                    HttpStatusCode.OK,
                    jsonHeaders,
                )
                "/engine-rest/migration/generate" -> {
                    val source = body(request.body).getValue("sourceProcessDefinitionId").jsonPrimitive.content
                    if (source == "def-0") {
                        respond("""{"message":"activity removed"}""", HttpStatusCode.BadRequest, jsonHeaders)
                    } else {
                        respond("""{"sourceProcessDefinitionId":"$source","targetProcessDefinitionId":"def-2","instructions":[]}""", HttpStatusCode.OK, jsonHeaders)
                    }
                }
                "/engine-rest/migration/execute" -> {
                    executed += body(request.body)
                    respond("", HttpStatusCode.NoContent)
                }
                else -> error("unexpected request ${request.url}")
            }
        }
        val backend = Camunda7Backend(client = Camunda7Client("http://engine/engine-rest", null, engine))

        val result = backend.migrate(versioned)

        assertEquals(1, result.migrated)
        assertEquals(listOf("pi-3"), result.failures.single().instanceIds)
        assertTrue("activity removed" in result.failures.single().error, result.failures.single().error)
        val execution = executed.single()
        assertEquals(listOf("pi-1"), execution.getValue("processInstanceIds").jsonArray.map { it.jsonPrimitive.content })
        assertEquals("def-1", execution.getValue("migrationPlan").jsonObject.getValue("sourceProcessDefinitionId").jsonPrimitive.content)
        assertEquals(MigrationResult(1, listOf(MigrationFailure("c7-versioned", listOf("pi-3"), result.failures.single().error))), result)
    }
}
