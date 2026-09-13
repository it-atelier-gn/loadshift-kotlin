package loadshift.web

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.basicAuth
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import loadshift.core.RunConfig
import loadshift.core.Start
import loadshift.core.WorkItem
import loadshift.core.task
import loadshift.core.workflow
import loadshift.local.LocalBackend
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Serializable
private data class Doc(var id: String) : WorkItem {
    override val key get() = id
}

class ControlServerTest {

    @Test
    fun servesApiAndUi() = runBlocking {
        val backend = LocalBackend()
        val wf = workflow<Doc>("web-flow") {
            input(listOf(Doc("a"), Doc("b")))
            task("noop") {}
        }
        backend.run(wf).await()

        val server = ControlServer(backend, port = 0).start()
        val port = server.boundPort()
        val client = HttpClient(CIO) {
            install(ContentNegotiation) { json() }
        }
        try {
            val info: BackendDto = client.get("http://127.0.0.1:$port/api/backend").body()
            assertEquals("local", info.type)
            assertEquals(1, info.runCount)

            val runs: List<RunDto> = client.get("http://127.0.0.1:$port/api/runs").body()
            assertEquals(1, runs.size)
            val run = runs.single()
            assertEquals("web-flow", run.workflowName)
            assertEquals("Completed", run.state)
            assertEquals(2, run.progress.done)

            val detail: RunDetailDto = client.get("http://127.0.0.1:$port/api/runs/${run.id}").body()
            assertEquals(run.id, detail.run.id)
            val structure = assertNotNull(detail.structure)
            assertEquals("workflow", structure.type)

            assertEquals(404, client.get("http://127.0.0.1:$port/api/runs/missing").status.value)

            val html = client.get("http://127.0.0.1:$port/").bodyAsText()
            assertTrue(html.contains("loadshift"))
        } finally {
            client.close()
            server.stop()
        }
    }

    @Test
    fun startEndpointTriggersManualRun() = runBlocking {
        val backend = LocalBackend()
        val wf = workflow<Doc>("manual-flow") {
            input(listOf(Doc("a")))
            task("noop") {}
        }
        val handle = backend.run(wf, RunConfig(start = Start.Manual))

        val server = ControlServer(backend, port = 0).start()
        val port = server.boundPort()
        val client = HttpClient(CIO) {
            install(ContentNegotiation) { json() }
        }
        try {
            val runs: List<RunDto> = client.get("http://127.0.0.1:$port/api/runs").body()
            val id = runs.single().id
            assertEquals("Scheduled", runs.single().state)

            assertEquals(404, client.post("http://127.0.0.1:$port/api/runs/missing/start").status.value)

            assertEquals(200, client.post("http://127.0.0.1:$port/api/runs/$id/start").status.value)
            handle.await()

            val after: List<RunDto> = client.get("http://127.0.0.1:$port/api/runs").body()
            assertEquals("Completed", after.single().state)
        } finally {
            client.close()
            server.stop()
        }
    }

    @Test
    fun pauseAndResumeEndpointsControlARun() = runBlocking {
        val backend = LocalBackend()
        val wf = workflow<Doc>("pause-flow") {
            input(listOf(Doc("a")))
            task("noop") {}
        }
        val handle = backend.run(wf, RunConfig(start = Start.Manual))

        val server = ControlServer(backend, port = 0).start()
        val port = server.boundPort()
        val client = HttpClient(CIO) {
            install(ContentNegotiation) { json() }
        }
        suspend fun state(): String = client.get("http://127.0.0.1:$port/api/runs").body<List<RunDto>>().single().state
        try {
            val id = client.get("http://127.0.0.1:$port/api/runs").body<List<RunDto>>().single().id

            assertEquals(200, client.post("http://127.0.0.1:$port/api/runs/$id/pause").status.value)
            assertEquals(200, client.post("http://127.0.0.1:$port/api/runs/$id/start").status.value)
            withTimeout(5.seconds) { while (state() != "Paused") delay(20.milliseconds) }

            assertEquals(404, client.post("http://127.0.0.1:$port/api/runs/missing/resume").status.value)
            assertEquals(200, client.post("http://127.0.0.1:$port/api/runs/$id/resume").status.value)
            handle.await()

            assertEquals("Completed", state())
        } finally {
            client.close()
            server.stop()
        }
    }

    @Test
    fun credentialsProtectTheDashboardAndApi() = runBlocking {
        val credentials = ConsoleCredentials("ops", "s3cret")
        val server = ControlServer(LocalBackend(), port = 0, credentials = credentials).start()
        val port = server.boundPort()
        val client = HttpClient(CIO)
        try {
            assertEquals(401, client.get("http://127.0.0.1:$port/").status.value)
            assertEquals(401, client.get("http://127.0.0.1:$port/api/runs").status.value)
            assertEquals(401, client.get("http://127.0.0.1:$port/api/runs") { basicAuth("ops", "wrong") }.status.value)
            assertEquals(200, client.get("http://127.0.0.1:$port/api/runs") { basicAuth("ops", "s3cret") }.status.value)
            assertFalse("s3cret" in credentials.toString())
        } finally {
            client.close()
            server.stop()
        }
    }

    @Test
    fun detachEndpointStopsARun() = runBlocking {
        val backend = LocalBackend()
        val wf = workflow<Doc>("detach-flow") {
            input(listOf(Doc("a")))
            task("noop") {}
        }
        val handle = backend.run(wf, RunConfig(start = Start.Manual))

        val server = ControlServer(backend, port = 0).start()
        val port = server.boundPort()
        val client = HttpClient(CIO) {
            install(ContentNegotiation) { json() }
        }
        try {
            val id = client.get("http://127.0.0.1:$port/api/runs").body<List<RunDto>>().single().id
            assertEquals(404, client.post("http://127.0.0.1:$port/api/runs/missing/detach").status.value)
            assertEquals(200, client.post("http://127.0.0.1:$port/api/runs/$id/detach").status.value)
            handle.await()
            assertEquals("Cancelled", client.get("http://127.0.0.1:$port/api/runs").body<List<RunDto>>().single().state)
        } finally {
            client.close()
            server.stop()
        }
    }

    @Test
    fun cancelEndpointStopsManualRun() = runBlocking {
        val backend = LocalBackend()
        val wf = workflow<Doc>("cancel-flow") {
            input(listOf(Doc("a")))
            task("noop") {}
        }
        val handle = backend.run(wf, RunConfig(start = Start.Manual))

        val server = ControlServer(backend, port = 0).start()
        val port = server.boundPort()
        val client = HttpClient(CIO) {
            install(ContentNegotiation) { json() }
        }
        try {
            val runs: List<RunDto> = client.get("http://127.0.0.1:$port/api/runs").body()
            val id = runs.single().id

            assertEquals(200, client.post("http://127.0.0.1:$port/api/runs/$id/cancel").status.value)
            handle.await()

            val after: List<RunDto> = client.get("http://127.0.0.1:$port/api/runs").body()
            assertEquals("Cancelled", after.single().state)
        } finally {
            client.close()
            server.stop()
        }
    }
}
