package loadshift.web

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.runBlocking
import loadshift.core.WorkItemBase
import loadshift.core.workflow
import loadshift.local.LocalBackend
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

private class Doc : WorkItemBase() {
    var id: String by required()
    override val key get() = id
}

private fun doc(id: String) = Doc().apply { this.id = id }

class IntrospectionServerTest {

    @Test
    fun servesApiAndUi() = runBlocking {
        val backend = LocalBackend()
        val wf = workflow<Doc>("web-flow") {
            input(listOf(doc("a"), doc("b")))
            task("noop") {}
        }
        backend.run(wf).await()

        val server = IntrospectionServer(backend, port = 0).start()
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
}
