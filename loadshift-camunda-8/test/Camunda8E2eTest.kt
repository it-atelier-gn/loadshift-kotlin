package loadshift.camunda8

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import loadshift.core.RunConfig
import loadshift.core.RunState
import loadshift.core.WorkItemBase
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

private class EUser : WorkItemBase() {
    var id: String by required()
    var note: String? by optional()
    override val key get() = id
}

private class EContact : WorkItemBase() {
    var label: String by required()
    override val key get() = label
}

private fun user(id: String) = EUser().apply { this.id = id }

private fun contact(label: String) = EContact().apply { this.label = label }

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
        val base = Camunda8Engine.base
        if (base == null) {
            println("SKIP Camunda8E2eTest: Docker not available and LOADSHIFT_C8_BASE not set")
            return@runBlocking
        }
        purgeActiveInstances(base)

        val seen = Collections.synchronizedList(mutableListOf<String>())
        val key = "e2e8x${System.currentTimeMillis()}"
        val wf = workflow<EUser>(key) {
            input(listOf(user("a"), user("b")))
            task("stamp") { it.note = "ok:${it.id}" }
            fanOut<EContact>(expand = { u -> listOf(contact("${u.id}-1"), contact("${u.id}-2")) }) {
                condition({ it.label.endsWith("1") }) {
                    task("first") { c -> seen += "first:${c.label}" }
                } otherwise {
                    task("second") { c -> seen += "second:${c.label}" }
                }
            }
        }

        val backend = Camunda8Backend(base)
        val handle = backend.run(wf, RunConfig(maxConcurrency = 4))
        val result = withTimeout(180_000) { handle.await() }

        assertEquals(0, result.failed)
        assertEquals(12, result.done)
        assertEquals(
            setOf("first:a-1", "second:a-2", "first:b-1", "second:b-2"),
            seen.toSet(),
        )

        val snap = backend.introspection.runs().single()
        assertEquals(RunState.Completed, snap.state)
        assertEquals(2, snap.progress.seeded)
    }
}
