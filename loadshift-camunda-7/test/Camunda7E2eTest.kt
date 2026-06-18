package loadshift.camunda7

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.Serializable
import loadshift.core.RunConfig
import loadshift.core.RunState
import loadshift.core.WorkItem
import loadshift.core.workflow
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
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

private fun purgeRunningInstances(base: String) {
    val http = java.net.http.HttpClient.newHttpClient()
    val list = http.send(
        HttpRequest.newBuilder(URI("$base/process-instance?maxResults=1000")).GET().build(),
        HttpResponse.BodyHandlers.ofString(),
    ).body()
    for (instance in Json.parseToJsonElement(list).jsonArray) {
        val id = instance.jsonObject["id"]?.jsonPrimitive?.content ?: continue
        http.send(
            HttpRequest.newBuilder(URI("$base/process-instance/$id?skipCustomListeners=true")).DELETE().build(),
            HttpResponse.BodyHandlers.discarding(),
        )
    }
}

private object Camunda7Engine {
    val base: String? by lazy {
        System.getenv("LOADSHIFT_C7_BASE")?.let { return@lazy it }
        if (!DockerClientFactory.instance().isDockerAvailable) return@lazy null
        val container = GenericContainer("camunda/camunda-bpm-platform:run-7.24.0")
            .withExposedPorts(8080)
            .waitingFor(
                Wait.forHttp("/engine-rest/version")
                    .forPort(8080)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)),
            )
        container.start()
        Runtime.getRuntime().addShutdownHook(Thread { container.stop() })
        "http://${container.host}:${container.getMappedPort(8080)}/engine-rest"
    }
}

class Camunda7E2eTest {

    @Test
    fun fullLoopThroughRealEngine() = runBlocking {
        val base = Camunda7Engine.base
        if (base == null) {
            println("SKIP Camunda7E2eTest: Docker not available and LOADSHIFT_C7_BASE not set")
            return@runBlocking
        }
        purgeRunningInstances(base)

        val seen = Collections.synchronizedList(mutableListOf<String>())
        val key = "e2e7x${System.currentTimeMillis()}"
        val wf = workflow<EUser>(key) {
            input(listOf(EUser("a"), EUser("b")))
            task("stamp") { it.note = "ok:${it.id}" }
            fanOut<EContact>(expand = { u -> listOf(EContact("${u.id}-1"), EContact("${u.id}-2")) }) {
                condition({ it.label.endsWith("1") }) {
                    task("first") { c -> seen += "first:${c.label}" }
                } otherwise {
                    task("second") { c -> seen += "second:${c.label}" }
                }
            }
        }

        val backend = Camunda7Backend(base)
        val handle = backend.run(wf, RunConfig(maxConcurrency = 4))
        val result = withTimeout(180_000) { handle.await() }

        assertEquals(0, result.failed)
        assertEquals(12, result.done)
        assertEquals(
            setOf("first:a-1", "second:a-2", "first:b-1", "second:b-2"),
            seen.toSet(),
        )

        val snap = backend.control.runs().single()
        assertEquals(RunState.Completed, snap.state)
        assertEquals(2, snap.progress.seeded)
    }
}
