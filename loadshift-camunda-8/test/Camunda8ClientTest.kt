package loadshift.camunda8

import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.forms.FormDataContent
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.TextContent
import io.ktor.http.formUrlEncode
import io.ktor.http.headersOf
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import loadshift.core.EngineApi
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@OptIn(EngineApi::class)
class Camunda8ClientTest {
    private val jsonHeaders = headersOf(HttpHeaders.ContentType, "application/json")
    private val credentials = Camunda8Auth.ClientCredentials(
        tokenUrl = "http://identity/token",
        clientId = "worker",
        clientSecret = "s3cret",
        audience = "zeebe-api",
    )

    private fun body(content: Any): JsonObject = Json.parseToJsonElement((content as TextContent).text).jsonObject

    @Test
    fun clientCredentialsTokenIsFetchedOnceAndReused() = runTest {
        val tokenRequests = mutableListOf<String>()
        val authorizations = mutableListOf<String?>()
        val engine = MockEngine { request ->
            if (request.url.host == "identity") {
                tokenRequests += (request.body as FormDataContent).formData.formUrlEncode()
                respond("""{"access_token":"t1","expires_in":300}""", HttpStatusCode.OK, jsonHeaders)
            } else {
                authorizations += request.headers[HttpHeaders.Authorization]
                respond("""{"page":{"totalItems":2}}""", HttpStatusCode.OK, jsonHeaders)
            }
        }
        val client = Camunda8Client("http://engine", credentials, engine)

        assertEquals(2, client.instanceCount("p"))
        assertEquals(2, client.instanceCount("p"))

        assertEquals(1, tokenRequests.size)
        assertTrue("grant_type=client_credentials" in tokenRequests.single())
        assertTrue("audience=zeebe-api" in tokenRequests.single())
        assertEquals<List<String?>>(listOf("Bearer t1", "Bearer t1"), authorizations)
    }

    @Test
    fun unauthorizedResponseRefreshesTheTokenAndRetriesOnce() = runTest {
        val issued = ArrayDeque(listOf("t1", "t2"))
        val authorizations = mutableListOf<String?>()
        val engine = MockEngine { request ->
            if (request.url.host == "identity") {
                respond("""{"access_token":"${issued.removeFirst()}","expires_in":300}""", HttpStatusCode.OK, jsonHeaders)
            } else {
                val authorization = request.headers[HttpHeaders.Authorization]
                authorizations += authorization
                if (authorization == "Bearer t1") {
                    respond("", HttpStatusCode.Unauthorized)
                } else {
                    respond("""{"page":{"totalItems":5}}""", HttpStatusCode.OK, jsonHeaders)
                }
            }
        }
        val client = Camunda8Client("http://engine", credentials, engine)

        assertEquals(5, client.instanceCount("p"))

        assertEquals<List<String?>>(listOf("Bearer t1", "Bearer t2"), authorizations)
        assertTrue(issued.isEmpty())
    }

    @Test
    fun bearerTokenIsSentUnchangedAndSecretsStayOutOfToString() = runTest {
        val authorizations = mutableListOf<String?>()
        val engine = MockEngine { request ->
            authorizations += request.headers[HttpHeaders.Authorization]
            respond("""{"page":{"totalItems":0}}""", HttpStatusCode.OK, jsonHeaders)
        }
        val bearer = Camunda8Auth.Bearer("static-token")
        Camunda8Client("http://engine", bearer, engine).instanceCount("p")

        assertEquals<List<String?>>(listOf("Bearer static-token"), authorizations)
        assertFalse("static-token" in bearer.toString())
        assertFalse("s3cret" in credentials.toString())
    }

    @Test
    fun activeRootsFollowSearchCursorsAndDecodeVariableValues() = runTest {
        val searches = mutableListOf<JsonObject>()
        val engine = MockEngine { request ->
            val query = body(request.body)
            when (request.url.encodedPath) {
                "/v2/process-instances/search" -> {
                    searches += query
                    val text = when (query.getValue("page").jsonObject["after"]?.jsonPrimitive?.content) {
                        null -> """{"items":[{"processInstanceKey":"1","state":"ACTIVE"}],"page":{"endCursor":"c1"}}"""
                        "c1" -> """{"items":[{"processInstanceKey":"2","state":"ACTIVE"}],"page":{"endCursor":"c2"}}"""
                        else -> """{"items":[],"page":{}}"""
                    }
                    respond(text, HttpStatusCode.OK, jsonHeaders)
                }
                "/v2/variables/search" -> respond(
                    """{"items":[{"processInstanceKey":"1","value":"\"a\""},{"processInstanceKey":"2","value":"\"\""}]}""",
                    HttpStatusCode.OK,
                    jsonHeaders,
                )
                else -> error("unexpected request ${request.url}")
            }
        }
        val driver = Camunda8Driver(Camunda8Client("http://engine", Camunda8Auth.None, engine), "worker")

        val roots = driver.activeRoots("proc")

        assertEquals(listOf("1" to "a", "2" to null), roots.map { it.id to it.itemKey })
        assertEquals(3, searches.size)
        assertEquals("proc", searches.first().getValue("filter").jsonObject.getValue("processDefinitionId").jsonPrimitive.content)
    }

    @Test
    fun broadcastCorrelatesEverySubscriptionOfTheWorkflowOnly() = runTest {
        val correlated = mutableListOf<String>()
        val engine = MockEngine { request ->
            when (request.url.encodedPath) {
                "/v2/message-subscriptions/search" -> respond(
                    """{"items":[{"correlationKey":"wf:a"},{"correlationKey":"wf:b"},{"correlationKey":"other:c"}]}""",
                    HttpStatusCode.OK,
                    jsonHeaders,
                )
                "/v2/messages/correlation" -> {
                    correlated += body(request.body).getValue("correlationKey").jsonPrimitive.content
                    respond("""{"messageKey":"1"}""", HttpStatusCode.OK, jsonHeaders)
                }
                else -> error("unexpected request ${request.url}")
            }
        }
        val driver = Camunda8Driver(Camunda8Client("http://engine", Camunda8Auth.None, engine), "worker")

        assertTrue(driver.correlate("go", "wf", null))
        assertEquals(setOf("wf:a", "wf:b"), correlated.toSet())
    }
}
