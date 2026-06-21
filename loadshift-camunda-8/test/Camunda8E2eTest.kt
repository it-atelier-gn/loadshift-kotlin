package loadshift.camunda8

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.Serializable
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.RunState
import loadshift.core.WorkItem
import loadshift.core.fanOut
import loadshift.core.task
import kotlin.time.Duration.Companion.seconds
import loadshift.core.workflow
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals

@Serializable
private data class EUser(var id: String, var note: String? = null) : WorkItem {
    override val key get() = id
}

@Serializable
private data class EContact(var label: String) : WorkItem {
    override val key get() = label
}

private fun purgeActiveInstances(base: String) {
    val http = java.net.http.HttpClient.newHttpClient()
    val search = HttpRequest.newBuilder(URI("$base/v2/process-instances/search"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("""{"filter":{"state":"ACTIVE"}}"""))
        .build()
    val body = http.send(search, HttpResponse.BodyHandlers.ofString()).body()
    val items = Json.parseToJsonElement(body).jsonObject["items"]?.jsonArray ?: return
    for (item in items) {
        val key = item.jsonObject["processInstanceKey"]?.jsonPrimitive?.content ?: continue
        http.send(
            HttpRequest.newBuilder(URI("$base/v2/process-instances/$key/cancellation"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build(),
            HttpResponse.BodyHandlers.discarding(),
        )
    }
}

private val h2Config = """
    camunda:
      security:
        authentication:
          method: "basic"
          unprotectedApi: true
        authorizations:
          enabled: false
      data:
        secondary-storage:
          type: rdbms
          rdbms:
            url: jdbc:h2:mem:camunda;DB_CLOSE_DELAY=-1
            username: sa
            password:
            flushInterval: PT0.5S
""".trimIndent()

private object Camunda8Engine {
    val base: String? by lazy {
        System.getenv("LOADSHIFT_C8_BASE")?.let { return@lazy it }
        if (!DockerClientFactory.instance().isDockerAvailable) return@lazy null
        val container = GenericContainer("camunda/camunda:8.9.8")
            .withExposedPorts(8080, 9600)
            .withCopyToContainer(Transferable.of(h2Config), "/usr/local/camunda/config/application.yaml")
            .waitingFor(
                Wait.forHttp("/v2/topology")
                    .forPort(8080)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)),
            )
        container.start()
        Runtime.getRuntime().addShutdownHook(Thread { container.stop() })
        "http://${container.host}:${container.getMappedPort(8080)}"
    }
}

class Camunda8E2eTest {

    @Test
    fun fullLoopThroughRealEngine() = runBlocking {
        val base = requireNotNull(Camunda8Engine.base) {
            "Camunda8E2eTest requires Docker or LOADSHIFT_C8_BASE"
        }
        purgeActiveInstances(base)

        val seen = Collections.synchronizedList(mutableListOf<String>())
        val totals = Collections.synchronizedList(mutableListOf<String>())
        val key = "e2e8x${System.currentTimeMillis()}"
        val wf = workflow<EUser>(key) {
            input(listOf(EUser("a"), EUser("b")))
            timeout(60.seconds) {
                task("stamp") { it.note = "ok:${it.id}" }
                wait(1.seconds)
            }
            awaitMessage("proceed")
            fanOut(expand = { u -> listOf(EContact("${u.id}-1"), EContact("${u.id}-2")) }, context = { it }) {
                task("process") { child ->
                    if (child.label.endsWith("1")) {
                        seen += "first:${child.label}:${context().note}"
                    } else {
                        seen += "second:${child.label}"
                    }
                }
            }.reduce(0, combine = { acc, c -> acc + c.label.length }) { u, total ->
                totals += "${u.id}:$total"
            }
        }

        val backend = Camunda8Backend(base)
        val handle = backend.run(wf, RunConfig(maxConcurrency = 4))
        val result = withTimeout(180_000) {
            val done = async { handle.await() }
            while (done.isActive) {
                handle.send("proceed", "a")
                handle.send("proceed", "b")
                delay(500)
            }
            done.await()
        }

        assertEquals(0, result.failed)
        assertEquals(10, result.done)
        assertEquals(
            setOf("first:a-1:ok:a", "second:a-2", "first:b-1:ok:b", "second:b-2"),
            seen.toSet(),
        )
        assertEquals(setOf("a:6", "b:6"), totals.toSet())

        val snap = backend.control.runs().single()
        assertEquals(RunState.Completed, snap.state)
        assertEquals(2, snap.progress.seeded)
    }

    @Test
    fun compensationRunsThroughRealEngine() = runBlocking {
        val base = requireNotNull(Camunda8Engine.base) {
            "Camunda8E2eTest requires Docker or LOADSHIFT_C8_BASE"
        }
        purgeActiveInstances(base)

        val compensated = Collections.synchronizedList(mutableListOf<String>())
        val key = "e2e8saga${System.currentTimeMillis()}"
        val wf = workflow<EUser>(key) {
            input(listOf(EUser("saga1")))
            task("charge") { } compensate { compensated += "refund:${it.id}" }
            task("ship") { throw RuntimeException("boom") }
        }

        val backend = Camunda8Backend(base)
        val handle = backend.run(wf, RunConfig(maxConcurrency = 4, retry = RetryPolicy(maxAttempts = 1)))
        withTimeout(120_000) {
            while (compensated.isEmpty()) delay(500)
        }
        handle.cancel()

        assertEquals(listOf("refund:saga1"), compensated.toList())
    }
}
