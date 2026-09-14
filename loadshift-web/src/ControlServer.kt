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
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.request.receiveText
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import loadshift.core.ControllableBackend
import loadshift.core.DeadLetterStore
import loadshift.core.LogReader
import loadshift.core.RunConfig
import loadshift.core.WorkItem
import loadshift.core.Workflow
import java.security.MessageDigest
import kotlin.time.Clock
import kotlin.time.Instant

class ConsoleCredentials(val username: String, val password: String) {
    internal fun matches(name: String, secret: String): Boolean {
        val nameMatches = MessageDigest.isEqual(name.toByteArray(Charsets.UTF_8), username.toByteArray(Charsets.UTF_8))
        val secretMatches = MessageDigest.isEqual(secret.toByteArray(Charsets.UTF_8), password.toByteArray(Charsets.UTF_8))
        return nameMatches and secretMatches
    }

    override fun toString(): String = "ConsoleCredentials(username=$username)"
}

class DeadLetterConsole(
    val store: DeadLetterStore,
    val workflows: List<Workflow<*>>,
    val requeueConfig: RunConfig = RunConfig(),
) {
    init {
        val duplicate = workflows.groupBy { it.key }.entries.firstOrNull { it.value.size > 1 }?.key
        require(duplicate == null) { "workflow '$duplicate' is listed more than once" }
    }
}

class ControlServer(
    private val backend: ControllableBackend,
    private val port: Int = 8571,
    private val host: String = "127.0.0.1",
    private val credentials: ConsoleCredentials? = null,
    private val deadLetters: DeadLetterConsole? = null,
    private val logs: LogReader? = null,
    private val oidc: ConsoleOidc? = null,
    private val userTasks: List<Workflow<*>> = emptyList(),
) {
    init {
        require(credentials == null || oidc == null) { "a console uses either credentials or oidc" }
    }

    private var server: EmbeddedServer<CIOApplicationEngine, CIOApplicationEngine.Configuration>? = null
    private var provider: OidcProvider? = null

    fun start(wait: Boolean = false): ControlServer {
        val discovered = oidc?.let { runBlocking { OidcProvider.discover(it) } }
        provider = discovered
        server = embeddedServer(CIO, port = port, host = host) {
            console(backend, credentials, deadLetters, logs, discovered, userTasks)
        }.also { it.start(wait) }
        return this
    }

    suspend fun boundPort(): Int =
        checkNotNull(server) { "server not started" }.engine.resolvedConnectors().first().port

    fun stop() {
        server?.stop(gracePeriodMillis = 100, timeoutMillis = 1000)
        server = null
        provider?.close()
        provider = null
    }
}

private const val CONSOLE_AUTH = "loadshift-console"
private const val DEFAULT_PAGE = 50
private const val MAX_PAGE = 500

internal fun Application.console(
    backend: ControllableBackend,
    credentials: ConsoleCredentials? = null,
    deadLetters: DeadLetterConsole? = null,
    logs: LogReader? = null,
    oidc: OidcProvider? = null,
    userTasks: List<Workflow<*>> = emptyList(),
) {
    install(ContentNegotiation) { json() }
    if (credentials != null) {
        install(Authentication) {
            basic(CONSOLE_AUTH) {
                realm = "loadshift console"
                validate { if (credentials.matches(it.name, it.password)) UserIdPrincipal(it.name) else null }
            }
        }
    }
    if (oidc != null) installOidc(oidc)
    routing {
        val routes: Route.() -> Unit = { consoleRoutes(backend, deadLetters, logs, userTasks) }
        when {
            oidc != null -> oidcRoutes(oidc, routes)
            credentials != null -> authenticate(CONSOLE_AUTH) { routes() }
            else -> routes()
        }
    }
}

private fun Route.consoleRoutes(
    backend: ControllableBackend,
    deadLetters: DeadLetterConsole?,
    logs: LogReader?,
    userTasks: List<Workflow<*>>,
) {
    staticResources("/", "web") {
        default("index.html")
    }
    route("/api") {
        get("/backend") {
            call.respond(BackendDto(backend.control.backendType, backend.control.runs().size))
        }
        get("/runs") {
            val control = backend.control
            val now = Clock.System.now()
            val local = control.runs().map { it.toDto(control.worker) }
            val recorded = control.registeredRuns().map { it.toDto(now) }
            call.respond((local + recorded).sortedBy { Instant.parse(it.startedAt) })
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
        get("/runs/{id}/items/{key}") {
            val status = backend.control.item(call.parameters["id"].orEmpty(), call.parameters["key"].orEmpty())
            if (status == null) call.respond(HttpStatusCode.NotFound) else call.respond(status.toDto())
        }
        post("/runs/{id}/items/{key}/cancel") {
            val cancelled = backend.control.cancelItem(call.parameters["id"].orEmpty(), call.parameters["key"].orEmpty())
            call.respond(if (cancelled) HttpStatusCode.OK else HttpStatusCode.NotFound)
        }
        get("/runs/{id}/logs") {
            val reader = logs ?: return@get call.respond(HttpStatusCode.NotFound)
            val runId = backend.control.run(call.parameters["id"].orEmpty())?.runId
                ?: return@get call.respond(HttpStatusCode.NotFound)
            val limit = call.request.queryParameters["limit"]?.toIntOrNull() ?: DEFAULT_PAGE
            if (limit !in 1..MAX_PAGE) return@get call.respond(HttpStatusCode.BadRequest)
            val page = try {
                reader.list(runId, limit, call.request.queryParameters["after"])
            } catch (e: IllegalArgumentException) {
                return@get call.respond(HttpStatusCode.BadRequest)
            }
            call.respond(page.toDto())
        }
        get("/runs/{id}/dead-letters") {
            val store = deadLetters?.store ?: return@get call.respond(HttpStatusCode.NotFound)
            val runId = backend.control.run(call.parameters["id"].orEmpty())?.runId
                ?: return@get call.respond(HttpStatusCode.NotFound)
            val limit = call.request.queryParameters["limit"]?.toIntOrNull() ?: DEFAULT_PAGE
            if (limit !in 1..MAX_PAGE) return@get call.respond(HttpStatusCode.BadRequest)
            val page = try {
                store.forRun(runId, limit, call.request.queryParameters["after"])
            } catch (e: IllegalArgumentException) {
                return@get call.respond(HttpStatusCode.BadRequest)
            }
            call.respond(page.toDto())
        }
        get("/user-tasks") {
            if (userTasks.isEmpty()) return@get call.respond(HttpStatusCode.NotFound)
            call.respond(userTasks.flatMap { backend.userTasks(it) }.distinctBy { it.id }.map { it.toDto() })
        }
        post("/user-tasks/{id}/complete") {
            if (userTasks.isEmpty()) return@post call.respond(HttpStatusCode.NotFound)
            val text = call.receiveText()
            val form = if (text.isBlank()) {
                JsonObject(emptyMap())
            } else {
                runCatching { Json.parseToJsonElement(text) as? JsonObject }.getOrNull()
                    ?: return@post call.respond(HttpStatusCode.BadRequest)
            }
            val completed = backend.completeUserTask(call.parameters["id"].orEmpty(), form)
            call.respond(if (completed) HttpStatusCode.NoContent else HttpStatusCode.NotFound)
        }
        deadLetterRoutes(backend, deadLetters)
    }
}

private fun Route.deadLetterRoutes(backend: ControllableBackend, console: DeadLetterConsole?) {
    route("/dead-letters") {
        get("/workflows") {
            if (console == null) return@get call.respond(HttpStatusCode.NotFound)
            call.respond(console.workflows.map { it.toDto() })
        }
        get {
            if (console == null) return@get call.respond(HttpStatusCode.NotFound)
            val workflow = call.request.queryParameters["workflow"]
            val limit = call.request.queryParameters["limit"]?.toIntOrNull() ?: DEFAULT_PAGE
            if (workflow.isNullOrBlank() || limit !in 1..MAX_PAGE) return@get call.respond(HttpStatusCode.BadRequest)
            val page = try {
                console.store.list(workflow, limit, call.request.queryParameters["after"])
            } catch (e: IllegalArgumentException) {
                return@get call.respond(HttpStatusCode.BadRequest)
            }
            call.respond(page.toDto())
        }
        post("/requeue") {
            if (console == null) return@post call.respond(HttpStatusCode.NotFound)
            val workflowKey = call.request.queryParameters["workflow"]
            val itemKey = call.request.queryParameters["item"]
            if (workflowKey.isNullOrBlank() || itemKey.isNullOrBlank()) return@post call.respond(HttpStatusCode.BadRequest)
            @Suppress("UNCHECKED_CAST")
            val workflow = console.workflows.firstOrNull { it.key == workflowKey } as Workflow<WorkItem>?
                ?: return@post call.respond(HttpStatusCode.NotFound)
            val records = console.store.forKey(workflowKey, itemKey)
            if (records.isEmpty()) return@post call.respond(HttpStatusCode.NotFound)
            backend.requeue(workflow, records, console.requeueConfig.copy(deadLetters = console.store))
            call.respond(HttpStatusCode.Accepted)
        }
        post("/{id}/requeue") {
            if (console == null) return@post call.respond(HttpStatusCode.NotFound)
            val record = console.store.get(call.parameters["id"].orEmpty()) ?: return@post call.respond(HttpStatusCode.NotFound)
            @Suppress("UNCHECKED_CAST")
            val workflow = console.workflows.firstOrNull { it.key == record.workflowKey } as Workflow<WorkItem>?
                ?: return@post call.respond(HttpStatusCode.NotFound)
            backend.requeue(workflow, listOf(record), console.requeueConfig.copy(deadLetters = console.store))
            call.respond(HttpStatusCode.Accepted)
        }
        delete("/{id}") {
            if (console == null) return@delete call.respond(HttpStatusCode.NotFound)
            val id = call.parameters["id"].orEmpty()
            if (console.store.get(id) == null) return@delete call.respond(HttpStatusCode.NotFound)
            console.store.remove(id)
            call.respond(HttpStatusCode.NoContent)
        }
    }
}

private fun Route.command(name: String, action: suspend (String) -> Boolean) {
    post("/runs/{id}/$name") {
        val id = call.parameters["id"].orEmpty()
        call.respond(if (action(id)) HttpStatusCode.OK else HttpStatusCode.NotFound)
    }
}
