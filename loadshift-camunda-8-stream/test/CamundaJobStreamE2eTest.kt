package loadshift.camunda8.stream

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import loadshift.camunda8.Camunda8Backend
import loadshift.camunda8.Camunda8JobStream
import loadshift.camunda8.StreamedJob
import loadshift.core.WorkItem
import loadshift.core.task
import loadshift.core.workflow
import org.junit.jupiter.api.Assumptions
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import java.net.URI
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

@Serializable
private data class Parcel(var id: String, var packed: Boolean = false) : WorkItem {
    override val key get() = id
}

private val image = System.getenv("LOADSHIFT_C8_IMAGE") ?: "camunda/camunda:8.9.19"

private val engineConfig = """
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

private object StreamingEngine {
    val addresses: Pair<URI, URI>? by lazy {
        if (!DockerClientFactory.instance().isDockerAvailable) return@lazy null
        val container = GenericContainer(image)
            .withExposedPorts(8080, 26500, 9600)
            .withCopyToContainer(Transferable.of(engineConfig), "/usr/local/camunda/config/application.yaml")
            .waitingFor(Wait.forHttp("/v2/topology").forPort(8080).forStatusCode(200).withStartupTimeout(java.time.Duration.ofMinutes(3)))
        container.start()
        Runtime.getRuntime().addShutdownHook(Thread { container.stop() })
        URI("http://${container.host}:${container.getMappedPort(8080)}") to URI("http://${container.host}:${container.getMappedPort(26500)}")
    }
}

class CamundaJobStreamE2eTest {

    @Test
    fun jobsPushedOverTheStreamAreWorked() = runBlocking {
        val (rest, grpc) = StreamingEngine.addresses ?: Assumptions.abort("$image tests require Docker")
        CamundaJobStream(grpc, rest).use { stream ->
            val pushed = AtomicInteger()
            val endings = Collections.synchronizedList(mutableListOf<Throwable?>())
            val counting = object : Camunda8JobStream {
                override fun open(
                    jobType: String,
                    worker: String,
                    timeout: Duration,
                    tenantId: String?,
                    receive: (StreamedJob) -> Unit,
                    ended: (Throwable?) -> Unit,
                ) = stream.open(
                    jobType,
                    worker,
                    timeout,
                    tenantId,
                    { job ->
                        pushed.incrementAndGet()
                        receive(job)
                    },
                ) { error ->
                    endings += error
                    ended(error)
                }
            }
            val wf = workflow<Parcel>("stream-${System.nanoTime()}") {
                input((1..20).map { Parcel("p$it") })
                task("pack") { it.packed = true }
                task("ship") { check(it.packed) }
            }

            val result = withTimeout(3.minutes) { Camunda8Backend(rest.toString(), jobStream = counting).run(wf).await() }

            assertEquals(20, result.done)
            assertTrue(result.deadLetters.isEmpty(), result.deadLetters.toString())
            assertTrue(endings.isEmpty(), endings.joinToString { it?.stackTraceToString() ?: "ended without error" })
            assertTrue(pushed.get() > 0, "no job arrived through the stream")
        }
    }
}
