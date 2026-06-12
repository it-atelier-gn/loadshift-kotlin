package loadshift.web

import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.cio.CIOApplicationEngine
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.http.content.staticResources
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import loadshift.core.IntrospectableBackend

class IntrospectionServer(
    private val backend: IntrospectableBackend,
    private val port: Int = 8571,
    private val host: String = "127.0.0.1",
) {
    private var server: EmbeddedServer<CIOApplicationEngine, CIOApplicationEngine.Configuration>? = null

    fun start(wait: Boolean = false): IntrospectionServer {
        server = embeddedServer(CIO, port = port, host = host) { console(backend) }.also { it.start(wait) }
        return this
    }

    suspend fun boundPort(): Int =
        checkNotNull(server) { "server not started" }.engine.resolvedConnectors().first().port

    fun stop() {
        server?.stop(gracePeriodMillis = 100, timeoutMillis = 1000)
        server = null
    }
}

internal fun Application.console(backend: IntrospectableBackend) {
    install(ContentNegotiation) { json() }
    routing {
        staticResources("/", "web") {
            default("index.html")
        }
        route("/api") {
            get("/backend") {
                call.respond(BackendDto(backend.introspection.backendType, backend.introspection.runs().size))
            }
            get("/runs") {
                call.respond(backend.introspection.runs().map { it.toDto() })
            }
            get("/runs/{id}") {
                val id = call.parameters["id"].orEmpty()
                val snapshot = backend.introspection.run(id)
                if (snapshot == null) {
                    call.respond(HttpStatusCode.NotFound)
                } else {
                    call.respond(RunDetailDto(snapshot.toDto(), backend.introspection.structure(id)?.toDto()))
                }
            }
        }
    }
}
