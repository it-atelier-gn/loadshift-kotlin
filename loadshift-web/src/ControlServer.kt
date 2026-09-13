package loadshift.web

import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.auth.Authentication
import io.ktor.server.auth.UserIdPrincipal
import io.ktor.server.auth.authenticate
import io.ktor.server.auth.basic
import io.ktor.server.cio.CIO
import io.ktor.server.cio.CIOApplicationEngine
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.http.content.staticResources
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import loadshift.core.ControllableBackend
import java.security.MessageDigest

class ConsoleCredentials(val username: String, val password: String) {
    internal fun matches(name: String, secret: String): Boolean {
        val nameMatches = MessageDigest.isEqual(name.toByteArray(Charsets.UTF_8), username.toByteArray(Charsets.UTF_8))
        val secretMatches = MessageDigest.isEqual(secret.toByteArray(Charsets.UTF_8), password.toByteArray(Charsets.UTF_8))
        return nameMatches and secretMatches
    }

    override fun toString(): String = "ConsoleCredentials(username=$username)"
}

class ControlServer(
    private val backend: ControllableBackend,
    private val port: Int = 8571,
    private val host: String = "127.0.0.1",
    private val credentials: ConsoleCredentials? = null,
) {
    private var server: EmbeddedServer<CIOApplicationEngine, CIOApplicationEngine.Configuration>? = null

    fun start(wait: Boolean = false): ControlServer {
        server = embeddedServer(CIO, port = port, host = host) { console(backend, credentials) }.also { it.start(wait) }
        return this
    }

    suspend fun boundPort(): Int =
        checkNotNull(server) { "server not started" }.engine.resolvedConnectors().first().port

    fun stop() {
        server?.stop(gracePeriodMillis = 100, timeoutMillis = 1000)
        server = null
    }
}

private const val CONSOLE_AUTH = "loadshift-console"

internal fun Application.console(backend: ControllableBackend, credentials: ConsoleCredentials? = null) {
    install(ContentNegotiation) { json() }
    if (credentials != null) {
        install(Authentication) {
            basic(CONSOLE_AUTH) {
                realm = "loadshift console"
                validate { if (credentials.matches(it.name, it.password)) UserIdPrincipal(it.name) else null }
            }
        }
    }
    routing {
        if (credentials == null) consoleRoutes(backend) else authenticate(CONSOLE_AUTH) { consoleRoutes(backend) }
    }
}

private fun Route.consoleRoutes(backend: ControllableBackend) {
    staticResources("/", "web") {
        default("index.html")
    }
    route("/api") {
        get("/backend") {
            call.respond(BackendDto(backend.control.backendType, backend.control.runs().size))
        }
        get("/runs") {
            call.respond(backend.control.runs().map { it.toDto() })
        }
        get("/runs/{id}") {
            val id = call.parameters["id"].orEmpty()
            val snapshot = backend.control.run(id)
            if (snapshot == null) {
                call.respond(HttpStatusCode.NotFound)
            } else {
                call.respond(RunDetailDto(snapshot.toDto(), backend.control.structure(id)?.toDto()))
            }
        }
        command("start") { backend.control.start(it) }
        command("pause") { backend.control.pause(it) }
        command("resume") { backend.control.resume(it) }
        command("cancel") { backend.control.cancel(it) }
        command("detach") { backend.control.detach(it) }
    }
}

private fun Route.command(name: String, action: suspend (String) -> Boolean) {
    post("/runs/{id}/$name") {
        val id = call.parameters["id"].orEmpty()
        call.respond(if (action(id)) HttpStatusCode.OK else HttpStatusCode.NotFound)
    }
}
