package loadshift.camunda7

import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.TextContent
import io.ktor.http.headersOf
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import loadshift.core.EngineApi
import loadshift.core.EngineJob
import loadshift.core.EngineNames
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@OptIn(EngineApi::class)
class Camunda7ClientTest {
    private val jsonHeaders = headersOf(HttpHeaders.ContentType, "application/json")
    private val base = "http://engine/engine-rest"

    private fun body(content: Any): JsonObject = Json.parseToJsonElement((content as TextContent).text).jsonObject

    @Test
    fun sendsBasicCredentialsWithEveryRequest() = runTest {
        val seen = mutableListOf<String?>()
        val engine = MockEngine { request ->
            seen += request.headers[HttpHeaders.Authorization]
            respond("""{"count":3}""", HttpStatusCode.OK, jsonHeaders)
        }
        val credentials = BasicCredentials("demo", "secret")
        val client = Camunda7Client(base, credentials, engine)

        assertEquals(3, client.processInstanceCount("p"))
        assertEquals(3, client.processInstanceCount("q"))

        val expected = "Basic " + Base64.getEncoder().encodeToString("demo:secret".toByteArray())
        assertEquals<List<String?>>(listOf(expected, expected), seen)
        assertFalse("secret" in credentials.toString())
    }

    @Test
    fun activeRootsPageThroughInstancesAndReadTheirItemKeys() = runTest {
        val total = Camunda7Driver.PAGE_SIZE + 1
        val engine = MockEngine { request ->
            when (request.url.encodedPath) {
                "/engine-rest/process-instance" -> {
                    val first = request.url.parameters["firstResult"]!!.toInt()
                    val max = request.url.parameters["maxResults"]!!.toInt()
                    val page = (first until minOf(first + max, total)).joinToString(",", "[", "]") { """{"id":"pi-$it"}""" }
                    respond(page, HttpStatusCode.OK, jsonHeaders)
                }
                "/engine-rest/variable-instance" -> {
                    val ids = body(request.body).getValue("processInstanceIdIn").jsonArray.map { it.jsonPrimitive.content }
                    val variables = ids.joinToString(",", "[", "]") { id ->
                        val value = if (id == "pi-1") "" else "key-$id"
                        """{"processInstanceId":"$id","value":"$value","type":"String"}"""
                    }
                    respond(variables, HttpStatusCode.OK, jsonHeaders)
                }
                else -> error("unexpected request ${request.url}")
            }
        }
        val driver = Camunda7Driver(Camunda7Client(base, null, engine), "worker")

        val roots = driver.activeRoots("proc")

        assertEquals(total, roots.size)
        assertEquals("key-pi-0", roots.single { it.id == "pi-0" }.itemKey)
        assertNull(roots.single { it.id == "pi-1" }.itemKey)
        assertEquals("key-pi-${total - 1}", roots.single { it.id == "pi-${total - 1}" }.itemKey)
    }

    @Test
    fun terminateThrowsTheTerminateErrorWithOutcomeVariables() = runTest {
        var path: String? = null
        var sent: JsonObject? = null
        val engine = MockEngine { request ->
            path = request.url.encodedPath
            sent = body(request.body)
            respond("", HttpStatusCode.NoContent)
        }
        val driver = Camunda7Driver(Camunda7Client(base, null, engine), "worker-1")
        val job = EngineJob("task-1", "wf/ship", "pi-1", JsonObject(emptyMap()), null)

        driver.terminate(job, "no carrier", buildJsonObject { putJsonObject(EngineNames.OUTCOME) { put("topic", "ship") } })

        assertEquals("/engine-rest/external-task/task-1/bpmnError", path)
        val request = assertNotNull(sent)
        assertEquals(EngineNames.TERMINATE_ERROR, request.getValue("errorCode").jsonPrimitive.content)
        assertEquals("worker-1", request.getValue("workerId").jsonPrimitive.content)
        val outcome = request.getValue("variables").jsonObject.getValue(EngineNames.OUTCOME).jsonObject
        assertEquals("json", outcome.getValue("type").jsonPrimitive.content)
        assertTrue("ship" in outcome.getValue("value").jsonPrimitive.content)
    }

    @Test
    fun correlationReportsWhetherAnyInstanceReceivedTheMessage() = runTest {
        val responses = ArrayDeque(
            listOf(
                HttpStatusCode.OK to """[{"resultType":"Execution"}]""",
                HttpStatusCode.OK to "[]",
                HttpStatusCode.BadRequest to """{"message":"no match"}""",
            ),
        )
        val requests = mutableListOf<JsonObject>()
        val engine = MockEngine { request ->
            requests += body(request.body)
            val (status, text) = responses.removeFirst()
            respond(text, status, jsonHeaders)
        }
        val driver = Camunda7Driver(Camunda7Client(base, null, engine), "worker")

        assertTrue(driver.correlate("go", "wf", "a"))
        assertFalse(driver.correlate("go", "wf", "b"))
        assertFalse(driver.correlate("go", "wf", null))

        val targeted = requests[0].getValue("correlationKeys").jsonObject
        assertEquals("wf", targeted.getValue(EngineNames.WORKFLOW).jsonObject.getValue("value").jsonPrimitive.content)
        assertEquals("a", targeted.getValue(EngineNames.ITEM_KEY).jsonObject.getValue("value").jsonPrimitive.content)
        assertFalse(EngineNames.ITEM_KEY in requests[2].getValue("correlationKeys").jsonObject)
        assertTrue(requests.all { it.getValue("all").jsonPrimitive.content == "true" })
    }
}
