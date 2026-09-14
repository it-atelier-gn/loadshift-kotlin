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
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import loadshift.core.BpmnCompiler
import loadshift.core.WorkItem
import loadshift.core.task
import loadshift.core.workflow
import org.camunda.bpm.model.bpmn.Bpmn
import org.camunda.bpm.model.bpmn.instance.Process
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Serializable
private data class Shipment(var id: String = "") : WorkItem

class Camunda8MigrationTest {
    private val jsonHeaders = headersOf(HttpHeaders.ContentType, "application/json")

    private fun body(content: Any): JsonObject = Json.parseToJsonElement((content as TextContent).text).jsonObject

    private fun version(tag: String, extraTask: Boolean) = workflow<Shipment>("c8-versioned") {
        version(tag)
        input(emptyList())
        awaitMessage("go")
        if (extraTask) task("audit") { }
        task("ship") { }
    }

    @Test
    fun versionTagIsAZeebeExtensionOfEveryProcess() {
        val root = BpmnCompiler.compile(version("2", extraTask = false)).single()

        Camunda8Dialect.decorate(root.model, root.serviceTasks, root.versionTag)

        val process = root.model.getModelElementsByType(Process::class.java).single()
        val tag = process.extensionElements.domElement.childElements.single { it.localName == "versionTag" }
        assertEquals("2", tag.getAttribute("value"))
    }

    @Test
    fun migrateMapsTheElementsBothVersionsShareForEveryOutdatedInstance() = runTest {
        val previous = BpmnCompiler.compile(version("1", extraTask = false)).single()
        val previousXml = Bpmn.convertToString(previous.model)
        val migrations = mutableMapOf<String, JsonObject>()
        val engine = MockEngine { request ->
            val path = request.url.encodedPath
            when {
                path == "/v2/deployments" -> respond(
                    """{"deploymentKey":"9","tenantId":"<default>","deployments":[{"processDefinition":{"processDefinitionId":"c8-versioned","processDefinitionKey":"200","processDefinitionVersion":2}}]}""",
                    HttpStatusCode.OK,
                    jsonHeaders,
                )
                path == "/v2/process-instances/search" -> respond(
                    """{"items":[{"processInstanceKey":"10","state":"ACTIVE","processDefinitionKey":"100"},{"processInstanceKey":"11","state":"ACTIVE","processDefinitionKey":"200"}],"page":{}}""",
                    HttpStatusCode.OK,
                    jsonHeaders,
                )
                path == "/v2/process-definitions/100/xml" -> respond(previousXml, HttpStatusCode.OK, headersOf(HttpHeaders.ContentType, "text/xml"))
                path.endsWith("/migration") -> {
                    migrations[path] = body(request.body)
                    respond("", HttpStatusCode.NoContent)
                }
                else -> error("unexpected request ${request.url}")
            }
        }
        val backend = Camunda8Backend(client = Camunda8Client("http://engine", Camunda8Auth.None, engine))

        val result = backend.migrate(version("2", extraTask = true))

        assertEquals(1, result.migrated)
        assertTrue(result.failures.isEmpty())
        val request = migrations.getValue("/v2/process-instances/10/migration")
        assertEquals("200", request.getValue("targetProcessDefinitionKey").jsonPrimitive.content)
        val mapped = request.getValue("mappingInstructions").jsonArray.map {
            it.jsonObject.getValue("sourceElementId").jsonPrimitive.content to it.jsonObject.getValue("targetElementId").jsonPrimitive.content
        }
        assertTrue(mapped.all { (source, target) -> source == target })
        val ids = mapped.map { it.first }
        assertTrue("start" in ids && "msg_msg1" in ids, ids.toString())
        assertFalse(ids.any { it.startsWith("ext_audit") }, ids.toString())
    }
}
