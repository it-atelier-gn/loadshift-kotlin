package loadshift.web

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.basicAuth
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.jsonPrimitive
import loadshift.core.InMemoryDeadLetterStore
import loadshift.core.InMemoryLogSink
import loadshift.core.InMemoryRunRegistry
import loadshift.core.log
import loadshift.core.Progress
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.RunRecord
import loadshift.core.RunState
import loadshift.core.Start
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
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
    fun userTasksAreListedAndCompletedThroughTheApi() = runBlocking {
        val approved = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Doc>("approval-flow") {
            input(listOf(Doc("a")))
            userTask("approve", assignee = "ops") { doc, form -> approved += "${doc.id}:${form.getValue("decision").jsonPrimitive.content}" }
        }
        val backend = LocalBackend()
        val handle = backend.run(wf)
        val server = ControlServer(backend, port = 0, userTasks = listOf(wf)).start()
        val plain = ControlServer(LocalBackend(), port = 0).start()
        val client = HttpClient(CIO) {
            install(ContentNegotiation) { json() }
        }
        val api = "http://127.0.0.1:${server.boundPort()}/api"
        try {
            val tasks = withTimeout(5.seconds) {
                var open = client.get("$api/user-tasks").body<List<UserTaskDto>>()
                while (open.isEmpty()) {
                    delay(20.milliseconds)
                    open = client.get("$api/user-tasks").body()
                }
                open
            }
            val task = tasks.single()
            assertEquals("approve", task.name)
            assertEquals("a", task.itemKey)
            assertEquals("ops", task.assignee)

            assertEquals(400, client.post("$api/user-tasks/${task.id}/complete") { setBody("not json") }.status.value)
            assertEquals(204, client.post("$api/user-tasks/${task.id}/complete") { setBody("""{"decision":"yes"}""") }.status.value)
            handle.await()
            assertEquals(listOf("a:yes"), approved.toList())
            assertEquals(404, client.post("$api/user-tasks/${task.id}/complete") { setBody("{}") }.status.value)
            assertEquals(404, client.get("http://127.0.0.1:${plain.boundPort()}/api/user-tasks").status.value)
        } finally {
            client.close()
            server.stop()
            plain.stop()
        }
    }

    @Test
    fun logEntriesAndDeadLetterRecordsAreServedPerRun() = runBlocking {
        val logs = InMemoryLogSink()
        val store = InMemoryDeadLetterStore()
        val wf = workflow<Doc>("drill-flow") {
            input(listOf(Doc("a"), Doc("b")))
            task("ship", retry = RetryPolicy.None) {
                log("shipping", "id" to it.id)
                if (it.id == "b") error("no carrier")
            }
        }
        val backend = LocalBackend()
        backend.run(wf, RunConfig(logSink = logs, deadLetters = store)).await()
        val bare = LocalBackend()
        bare.run(wf).await()

        val server = ControlServer(backend, port = 0, deadLetters = DeadLetterConsole(store, listOf(wf)), logs = logs).start()
        val plain = ControlServer(bare, port = 0).start()
        val client = HttpClient(CIO) {
            install(ContentNegotiation) { json() }
        }
        val api = "http://127.0.0.1:${server.boundPort()}/api"
        val plainApi = "http://127.0.0.1:${plain.boundPort()}/api"
        try {
            val id = client.get("$api/runs").body<List<RunDto>>().single().id
            val page = client.get("$api/runs/$id/logs?limit=1").body<LogPageDto>()
            assertEquals(listOf("shipping"), page.entries.map { it.message })
            val cursor = assertNotNull(page.nextCursor)
            assertEquals(1, client.get("$api/runs/$id/logs?after=$cursor").body<LogPageDto>().entries.size)

            val records = client.get("$api/runs/$id/dead-letters").body<DeadLetterPageDto>()
            assertEquals(listOf("b"), records.records.map { it.key })
            assertEquals("b", records.records.single().item.getValue("id").jsonPrimitive.content)

            assertEquals(400, client.get("$api/runs/$id/logs?limit=0").status.value)
            assertEquals(400, client.get("$api/runs/$id/logs?after=not-a-cursor").status.value)
            assertEquals(400, client.get("$api/runs/$id/dead-letters?after=not-a-cursor").status.value)
            assertEquals(404, client.get("$api/runs/missing/logs").status.value)
            assertEquals(404, client.get("$api/runs/missing/dead-letters").status.value)

            val plainId = client.get("$plainApi/runs").body<List<RunDto>>().single().id
            assertEquals(404, client.get("$plainApi/runs/$plainId/logs").status.value)
            assertEquals(404, client.get("$plainApi/runs/$plainId/dead-letters").status.value)
        } finally {
            client.close()
            server.stop()
            plain.stop()
        }
    }

    @Test
    fun runsOfOtherWorkersFromTheRegistryAreListedReadOnly() = runBlocking {
        val registry = InMemoryRunRegistry()
        val backend = LocalBackend(registry)
        backend.run(
            workflow<Doc>("local-flow") {
                input(listOf(Doc("a")))
                task("noop") {}
            },
        ).await()
        val now = Clock.System.now()
        registry.save(
            RunRecord(
                id = "other:run-1",
                worker = "other",
                backendType = "camunda8",
                workflowKey = "remote-flow",
                workflowName = "remote-flow",
                state = RunState.Running,
                startedAt = now - 2.minutes,
                updatedAt = now - 1.minutes,
                staleAt = now - 30.seconds,
                progress = Progress(seeded = 5, done = 2),
                deadLetters = 1,
            ),
        )

        val server = ControlServer(backend, port = 0).start()
        val port = server.boundPort()
        val client = HttpClient(CIO) {
            install(ContentNegotiation) { json() }
        }
        val api = "http://127.0.0.1:$port/api"
        try {
            val runs: List<RunDto> = client.get("$api/runs").body()
            assertEquals(2, runs.size)
            val remote = runs.first()
            assertEquals("other:run-1", remote.id)
            assertFalse(remote.controllable)
            assertTrue(remote.stale)
            assertEquals("other", remote.worker)
            assertEquals(1, remote.deadLetterCount)
            assertEquals(2, remote.progress.done)

            val local = runs.last()
            assertTrue(local.controllable)
            assertFalse(local.stale)
            assertEquals(backend.control.worker, local.worker)
            assertTrue(local.id.startsWith("${local.worker}:"), local.id)

            assertEquals(404, client.post("$api/runs/other:run-1/cancel").status.value)
            assertEquals(404, client.get("$api/runs/other:run-1").status.value)
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
    fun deadLetterEndpointsListRequeueAndDiscardRecords() = runBlocking {
        val store = InMemoryDeadLetterStore()
        val broken = AtomicBoolean(true)
        val processed = Collections.synchronizedList(mutableListOf<String>())
        val wf = workflow<Doc>("dlq-flow") {
            input(listOf(Doc("fixable"), Doc("hopeless")))
            task("work", retry = RetryPolicy.None) {
                if (it.id == "hopeless" || broken.get()) error("broken ${it.id}")
                processed += it.id
            }
        }
        val backend = LocalBackend()
        backend.run(wf, RunConfig(deadLetters = store)).await()
        broken.set(false)

        val server = ControlServer(backend, port = 0, deadLetters = DeadLetterConsole(store, listOf(wf))).start()
        val port = server.boundPort()
        val client = HttpClient(CIO) {
            install(ContentNegotiation) { json() }
        }
        val api = "http://127.0.0.1:$port/api/dead-letters"
        try {
            assertEquals(listOf(WorkflowDto("dlq-flow", "dlq-flow")), client.get("$api/workflows").body<List<WorkflowDto>>())
            val page = client.get("$api?workflow=dlq-flow&limit=10").body<DeadLetterPageDto>()
            assertEquals(setOf("fixable", "hopeless"), page.records.map { it.key }.toSet())
            val fixable = page.records.single { it.key == "fixable" }
            val hopeless = page.records.single { it.key == "hopeless" }
            assertEquals("work", fixable.topic)
            assertEquals("fixable", fixable.item["id"]?.jsonPrimitive?.content)

            assertEquals(400, client.get(api).status.value)
            assertEquals(400, client.get("$api?workflow=dlq-flow&after=bad").status.value)
            assertEquals(404, client.post("$api/missing/requeue").status.value)
            assertEquals(404, client.delete("$api/missing").status.value)

            assertEquals(202, client.post("$api/${fixable.id}/requeue").status.value)
            withTimeout(5.seconds) { while ("fixable" !in processed || store.get(fixable.id) != null) delay(20.milliseconds) }

            assertEquals(204, client.delete("$api/${hopeless.id}").status.value)
            assertEquals(emptyList(), client.get("$api?workflow=dlq-flow").body<DeadLetterPageDto>().records)
            assertEquals(2, backend.control.runs().size)
        } finally {
            client.close()
            server.stop()
        }
    }

    @Test
    fun itemEndpointsReportAndCancelItemsAndRequeueByItemKey() = runBlocking {
        val store = InMemoryDeadLetterStore()
        val wf = workflow<Doc>("item-flow") {
            input(listOf(Doc("waiting"), Doc("failing")))
            task("check", retry = RetryPolicy.None) { if (it.id == "failing") error("broken") }
            awaitMessage("never")
        }
        val backend = LocalBackend()
        val handle = backend.run(wf, RunConfig(deadLetters = store))

        val server = ControlServer(backend, port = 0, deadLetters = DeadLetterConsole(store, listOf(wf))).start()
        val port = server.boundPort()
        val client = HttpClient(CIO) {
            install(ContentNegotiation) { json() }
        }
        val api = "http://127.0.0.1:$port/api"
        try {
            val id = client.get("$api/runs").body<List<RunDto>>().single().id
            withTimeout(5.seconds) {
                while (client.get("$api/runs/$id/items/waiting").status.value != 200 ||
                    client.get("$api/runs/$id/items/waiting").body<ItemStatusDto>().topic != "check"
                ) delay(20.milliseconds)
            }
            assertEquals("Running", client.get("$api/runs/$id/items/waiting").body<ItemStatusDto>().state)
            assertEquals(404, client.get("$api/runs/$id/items/unknown").status.value)

            assertEquals(200, client.post("$api/runs/$id/items/waiting/cancel").status.value)
            handle.await()
            assertEquals("Cancelled", client.get("$api/runs/$id/items/waiting").body<ItemStatusDto>().state)
            assertEquals(404, client.post("$api/runs/$id/items/waiting/cancel").status.value)
            assertEquals(1, client.get("$api/runs").body<List<RunDto>>().single().progress.cancelled)

            assertEquals(400, client.post("$api/dead-letters/requeue?workflow=item-flow").status.value)
            assertEquals(404, client.post("$api/dead-letters/requeue?workflow=item-flow&item=waiting").status.value)
            assertEquals(202, client.post("$api/dead-letters/requeue?workflow=item-flow&item=failing").status.value)
            withTimeout(5.seconds) { while (backend.control.runs().size < 2) delay(20.milliseconds) }
        } finally {
            client.close()
            server.stop()
        }
    }

    @Test
    fun deadLetterEndpointsAreAbsentWithoutAStore() = runBlocking {
        val server = ControlServer(LocalBackend(), port = 0).start()
        val port = server.boundPort()
        val client = HttpClient(CIO)
        try {
            assertEquals(404, client.get("http://127.0.0.1:$port/api/dead-letters/workflows").status.value)
            assertEquals(404, client.get("http://127.0.0.1:$port/api/dead-letters?workflow=x").status.value)
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
