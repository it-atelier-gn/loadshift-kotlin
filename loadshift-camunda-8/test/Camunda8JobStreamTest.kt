package loadshift.camunda8

import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import loadshift.core.EngineApi
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(EngineApi::class)
class Camunda8JobStreamTest {
    private val jsonHeaders = headersOf(HttpHeaders.ContentType, "application/json")

    private class FakeStream : Camunda8JobStream {
        val receivers = Collections.synchronizedMap(mutableMapOf<String, (StreamedJob) -> Unit>())
        val endings = Collections.synchronizedMap(mutableMapOf<String, (Throwable?) -> Unit>())
        val opened = Collections.synchronizedList(mutableListOf<String>())
        val closed = Collections.synchronizedList(mutableListOf<String>())

        override fun open(
            jobType: String,
            worker: String,
            timeout: Duration,
            tenantId: String?,
            receive: (StreamedJob) -> Unit,
            ended: (Throwable?) -> Unit,
        ): AutoCloseable {
            opened += jobType
            receivers[jobType] = receive
            endings[jobType] = ended
            return AutoCloseable { closed += jobType }
        }

        fun push(type: String, key: String) =
            receivers.getValue(type)(StreamedJob(key, type, "pi-$key", 3, JsonObject(mapOf("id" to JsonPrimitive(key)))))
    }

    private fun engine(released: MutableList<String>, backlog: () -> String) = MockEngine { request ->
        when {
            request.url.encodedPath == "/v2/jobs/activation" -> respond("""{"jobs":${backlog()}}""", HttpStatusCode.OK, jsonHeaders)
            request.method == HttpMethod.Patch -> {
                released += request.url.encodedPath.substringAfterLast('/')
                respond("", HttpStatusCode.NoContent)
            }
            else -> error("unexpected request ${request.url}")
        }
    }

    @Test
    fun streamedJobsAreFetchedByTypeRestActivationCoversTheBacklogAndCloseReleasesBufferedJobs() = runBlocking {
        var activations = 0
        val released = Collections.synchronizedList(mutableListOf<String>())
        val client = engine(released) {
            activations++
            if (activations == 1) """[{"jobKey":"backlog","processInstanceKey":"pi-0","retries":3}]""" else "[]"
        }
        val stream = FakeStream()
        val driver = Camunda8Driver(Camunda8Client("http://engine", Camunda8Auth.None, client), "worker", stream)

        assertEquals(listOf("backlog"), driver.fetch(listOf("wf/a"), 5, 30.seconds, 50.milliseconds).map { it.id })
        assertEquals(listOf("wf/a"), stream.opened.toList())

        stream.push("wf/a", "1")
        stream.push("wf/a", "2")
        val pushed = driver.fetch(listOf("wf/a"), 5, 30.seconds, 50.milliseconds)
        assertEquals(listOf("1", "2"), pushed.map { it.id })
        assertEquals("pi-1", pushed.first().instanceId)
        assertEquals(1, activations)

        assertTrue(driver.fetch(listOf("wf/b"), 5, 30.seconds, 20.milliseconds).isEmpty())
        stream.push("wf/b", "3")
        assertTrue(driver.fetch(listOf("wf/a"), 5, 30.seconds, 20.milliseconds).isEmpty())

        driver.close()

        assertEquals(setOf("wf/a", "wf/b"), stream.closed.toSet())
        assertEquals(listOf("3"), released.toList())
        assertTrue(driver.fetch(listOf("wf/a"), 5, 30.seconds, 20.milliseconds).isEmpty())
    }

    @Test
    fun aStreamThatEndsIsOpenedAgainAfterTheReopenDelay() = runBlocking {
        val released = Collections.synchronizedList(mutableListOf<String>())
        val stream = FakeStream()
        val driver = Camunda8Driver(
            Camunda8Client("http://engine", Camunda8Auth.None, engine(released) { "[]" }),
            "worker",
            stream,
            streamReopenDelay = 200.milliseconds,
        )

        driver.fetch(listOf("wf/a"), 5, 30.seconds, 10.milliseconds)
        stream.endings.getValue("wf/a")(IllegalStateException("UNAVAILABLE"))
        driver.fetch(listOf("wf/a"), 5, 30.seconds, 10.milliseconds)
        assertEquals(listOf("wf/a"), stream.opened.toList())

        delay(250)
        driver.fetch(listOf("wf/a"), 5, 30.seconds, 10.milliseconds)
        assertEquals(listOf("wf/a", "wf/a"), stream.opened.toList())

        driver.close()
    }
}
