package loadshift.camunda8

import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.toByteArray
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.forms.FormDataContent
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.TextContent
import io.ktor.http.formUrlEncode
import io.ktor.http.headersOf
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import loadshift.core.EngineNames
import kotlinx.serialization.json.jsonPrimitive
import loadshift.core.EngineApi
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

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
    fun tenantIdScopesDeploymentInstancesJobsMessagesAndSearches() = runTest {
        val requests = mutableListOf<HttpRequestData>()
        val engine = MockEngine { request ->
            requests += request
            when (request.url.encodedPath) {
                "/v2/deployments" -> respond("{}", HttpStatusCode.OK, jsonHeaders)
                "/v2/process-instances" -> respond("""{"processInstanceKey":"1"}""", HttpStatusCode.OK, jsonHeaders)
                "/v2/jobs/activation" -> respond("""{"jobs":[]}""", HttpStatusCode.OK, jsonHeaders)
                "/v2/messages/correlation" -> respond("""{"messageKey":"1"}""", HttpStatusCode.OK, jsonHeaders)
                "/v2/process-instances/search" -> respond("""{"items":[],"page":{"totalItems":0}}""", HttpStatusCode.OK, jsonHeaders)
                "/v2/message-subscriptions/search" -> respond("""{"items":[]}""", HttpStatusCode.OK, jsonHeaders)
                else -> error("unexpected request ${request.url}")
            }
        }
        val client = Camunda8Client("http://engine", Camunda8Auth.None, engine, tenantId = "acme")
        val driver = Camunda8Driver(client, "worker")

        client.deploy(listOf("proc.bpmn" to "<definitions/>".toByteArray()))
        driver.startInstance("proc", JsonObject(emptyMap()), null)
        driver.fetch(listOf("wf/a"), 1, 1.seconds, Duration.ZERO)
        driver.correlate("go", "wf", "k", JsonObject(emptyMap()))
        client.instanceCount("proc")
        driver.activeRoots("proc")
        driver.correlate("go", "wf", null, JsonObject(emptyMap()))

        val deployment = String(requests[0].body.toByteArray())
        assertTrue("tenantId" in deployment && "acme" in deployment, deployment)
        assertEquals("acme", body(requests[1].body).getValue("tenantId").jsonPrimitive.content)
        assertEquals("acme", body(requests[2].body).getValue("tenantIds").jsonArray.single().jsonPrimitive.content)
        assertEquals("acme", body(requests[3].body).getValue("tenantId").jsonPrimitive.content)
        for (search in requests.subList(4, 7)) {
            assertEquals("acme", body(search.body).getValue("filter").jsonObject.getValue("tenantId").jsonPrimitive.content)
        }
    }

    @Test
    fun signalIsBroadcastByNameAndCarriesTheTenant() = runTest {
        val requests = mutableListOf<Pair<String, JsonObject>>()
        val engine = MockEngine { request ->
            requests += request.url.encodedPath to body(request.body)
            respond("""{"signalKey":"1"}""", HttpStatusCode.OK, jsonHeaders)
        }

        Camunda8Client("http://engine", Camunda8Auth.None, engine).broadcastSignal("stock-arrived")
        Camunda8Client("http://engine", Camunda8Auth.None, engine, tenantId = "acme").broadcastSignal("stock-arrived")

        assertEquals(listOf("/v2/signals/broadcast", "/v2/signals/broadcast"), requests.map { it.first })
        assertEquals("stock-arrived", requests[0].second.getValue("signalName").jsonPrimitive.content)
        assertFalse("tenantId" in requests[0].second)
        assertEquals("acme", requests[1].second.getValue("tenantId").jsonPrimitive.content)
    }

    @Test
    fun requestsCarryNoTenantWithoutATenantId() = runTest {
        val bodies = mutableListOf<JsonObject>()
        val engine = MockEngine { request ->
            bodies += body(request.body)
            respond("""{"processInstanceKey":"1","jobs":[]}""", HttpStatusCode.OK, jsonHeaders)
        }
        val driver = Camunda8Driver(Camunda8Client("http://engine", Camunda8Auth.None, engine), "worker")

        driver.startInstance("proc", JsonObject(emptyMap()), null)
        driver.fetch(listOf("wf/a"), 1, 1.seconds, Duration.ZERO)

        assertFalse("tenantId" in bodies[0])
        assertFalse("tenantIds" in bodies[1])
    }

    @Test
    fun broadcastCorrelatesEverySubscriptionOfTheWorkflowOnly() = runTest {
        val correlated = mutableListOf<String>()
        val payloads = mutableListOf<JsonObject?>()
        val engine = MockEngine { request ->
            when (request.url.encodedPath) {
                "/v2/message-subscriptions/search" -> respond(
                    """{"items":[{"correlationKey":"wf:a"},{"correlationKey":"wf:b"},{"correlationKey":"other:c"}]}""",
                    HttpStatusCode.OK,
                    jsonHeaders,
                )
                "/v2/messages/correlation" -> {
                    val sent = body(request.body)
                    correlated += sent.getValue("correlationKey").jsonPrimitive.content
                    payloads += sent["variables"]?.jsonObject
                    respond("""{"messageKey":"1"}""", HttpStatusCode.OK, jsonHeaders)
                }
                else -> error("unexpected request ${request.url}")
            }
        }
        val driver = Camunda8Driver(Camunda8Client("http://engine", Camunda8Auth.None, engine), "worker")

        val variables = buildJsonObject { put(EngineNames.messageVariable("go"), buildJsonObject { put("amount", 12) }) }
        assertTrue(driver.correlate("go", "wf", null, variables))
        assertEquals(setOf("wf:a", "wf:b"), correlated.toSet())
        assertEquals<List<JsonObject?>>(listOf(variables, variables), payloads)
    }
}
