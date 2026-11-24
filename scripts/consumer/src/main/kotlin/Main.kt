import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonPrimitive
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.opentelemetry.api.OpenTelemetry
import loadshift.camunda7.Camunda7Backend
import loadshift.camunda7.javaapi.Camunda7JavaApi
import loadshift.camunda8.stream.CamundaJobStream
import loadshift.micrometer.MicrometerMetrics
import loadshift.otel.OtelTracer
import loadshift.sqlite.SqliteDeadLetterStore
import loadshift.sqlite.SqliteLogSink
import loadshift.web.ControlServer
import org.camunda.bpm.engine.impl.cfg.StandaloneInMemProcessEngineConfiguration
import org.camunda.spin.plugin.impl.SpinProcessEnginePlugin
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import loadshift.camunda8.Camunda8Backend
import loadshift.core.Backend
import loadshift.core.DeadLetter
import loadshift.core.DeadLetterStore
import loadshift.core.InMemoryDeadLetterStore
import loadshift.core.ItemState
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.WorkItem
import loadshift.core.fanOut
import loadshift.core.task
import loadshift.core.workflow
import loadshift.local.LocalBackend
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

@Serializable
data class Order(val id: String, var total: Int = 0, var approved: Boolean = false, var paid: Int = 0, var billed: Boolean = false) : WorkItem {
    override val key get() = id
}

@Serializable
data class Line(val sku: String, val amount: Int, var priced: Boolean = false) : WorkItem {
    override val key get() = sku
}

@Serializable
data class Batch(val id: String, val items: List<Line>) : WorkItem {
    override val key get() = id
}

val failures = Collections.synchronizedList(mutableListOf<String>())

fun expect(name: String, condition: Boolean, detail: () -> String = { "" }) {
    if (condition) println("PASS $name") else {
        println("FAIL $name ${detail()}")
        failures += name
    }
}

suspend fun scenarioOrders(backend: Backend, tag: String, config: RunConfig = RunConfig(), store: DeadLetterStore = InMemoryDeadLetterStore()) {
    val refunds = Collections.synchronizedList(mutableListOf<String>())
    val pricedSeenParent = Collections.synchronizedSet(mutableSetOf<String>())
    val billing = workflow<Order>("billing-$tag") {
        input(emptyList())
        task("bill") { it.billed = true }
    }
    val shipped = Collections.synchronizedList(mutableListOf<String>())
    val wf = workflow<Order>("orders-$tag") {
        input(listOf(Order("o-1"), Order("o-2"), Order("o-bad")))
        task("reserve", retry = RetryPolicy(maxAttempts = 2, baseDelay = 100.milliseconds)) { } compensate { refunds += it.id }
        fanOut(expand = { o -> List(4) { Line("${o.id}-l$it", amount = it + 1) } }, context = { it }, concurrency = 2) {
            task("price") { line ->
                line.priced = true
                pricedSeenParent += "${line.sku}:${context().id}"
            }
        }.reduce(0, combine = { sum, line -> sum + line.amount }) { order, total -> order.total = total }
        userTask("Approve") { order, form -> order.approved = form["ok"]?.jsonPrimitive?.content == "true" }
        awaitMessage("paid") { order, data -> order.paid = data.getValue("amount").jsonPrimitive.int }
        call(billing)
        condition({ it.approved && it.paid == it.total && it.billed }) {
            task("ship", retry = RetryPolicy(maxAttempts = 2, baseDelay = 100.milliseconds)) { order ->
                if (order.id == "o-bad") error("no carrier for ${order.id}")
                shipped += order.id
            }
        }
    }
    val handle = backend.run(wf, config.copy(deadLetters = store))
    withTimeout(3.minutes) {
        val approved = mutableSetOf<String>()
        while (approved.size < 3) {
            for (t in backend.userTasks(wf)) {
                if (t.id in approved) continue
                if (backend.completeUserTask(t.id, JsonObject(mapOf("ok" to JsonPrimitive("true"))))) approved += t.id
            }
            delay(200.milliseconds)
        }
        for (id in listOf("o-1", "o-2", "o-bad")) handle.send("paid", id, JsonObject(mapOf("amount" to JsonPrimitive(10))))
    }
    val result = withTimeout(3.minutes) { handle.await() }
    expect("$tag orders done", result.done == 2L) { result.toString() }
    expect("$tag orders dead letter", result.deadLetters == listOf(DeadLetter("o-bad", "ship", "no carrier for o-bad"))) { result.deadLetters.toString() }
    expect("$tag orders compensated", refunds.toList() == listOf("o-bad")) { refunds.toString() }
    expect("$tag orders shipped", shipped.toSet() == setOf("o-1", "o-2")) { shipped.toString() }
    expect("$tag orders context", pricedSeenParent.size == 12 && pricedSeenParent.all { it.substringBefore("-l") == it.substringAfter(":") }) { pricedSeenParent.toString() }
    val records = store.list(wf.key).records
    expect("$tag orders dead letter stored", records.size == 1) { records.toString() }
    val status = handle.item("o-1")
    expect("$tag orders item status", status?.state == ItemState.Done) { status.toString() }
}

suspend fun scenarioBatch(backend: Backend, tag: String, size: Int) {
    val processed = Collections.synchronizedSet(mutableSetOf<String>())
    val running = AtomicInteger()
    val peak = AtomicInteger()
    val wf = workflow<Batch>("batch-$tag") {
        input(listOf(Batch("b-1", List(size) { Line("b-1-$it", 1) })))
        fanOut(expand = { b -> b.items }, context = { it }, concurrency = 10) {
            task("process") { line ->
                peak.accumulateAndGet(running.incrementAndGet()) { a, b -> maxOf(a, b) }
                check(context().items.size == size)
                processed += line.sku
                running.decrementAndGet()
            }
        }
    }
    val started = TimeSource.Monotonic.markNow()
    val result = withTimeout(10.minutes) { backend.run(wf, RunConfig(maxConcurrency = 16)).await() }
    println("INFO $tag batch $size took ${started.elapsedNow()}")
    expect("$tag batch done", result.done == 1L) { result.toString() }
    expect("$tag batch processed", processed.size == size) { "${processed.size}" }
    expect("$tag batch concurrency", peak.get() <= 10) { "${peak.get()}" }
}

suspend fun scenarioRestart(backend: Backend, tag: String) {
    fun build() = workflow<Order>("restart-$tag") {
        input(listOf(Order("r-1")))
        awaitMessage("go")
        task("finish") { }
    }
    val first = backend.run(build())
    withTimeout(1.minutes) { while (first.item("r-1")?.state != ItemState.Running) delay(200.milliseconds) }
    first.detach()
    val second = (backend as loadshift.core.EngineBackend).attach(build())
    second.send("go", "r-1")
    val result = withTimeout(2.minutes) { second.await() }
    expect("$tag attach finishes detached item", result.done == 1L) { result.toString() }
}

suspend fun scenarioReplicas(backends: List<Backend>, tag: String) {
    val handled = Collections.synchronizedList(mutableListOf<String>())
    val inboxes = backends.map { Channel<Order>(Channel.UNLIMITED) }
    fun service(inbox: Channel<Order>) = workflow<Order>("service-$tag") {
        input { inbox.receiveAsFlow() }
        awaitMessage("confirmed")
        task("handle") { handled += it.id }
    }
    val handles = backends.zip(inboxes).map { (backend, inbox) -> backend.run(service(inbox)) }
    for (n in 1..20) inboxes[n % inboxes.size].send(Order("s-$n"))
    withTimeout(2.minutes) {
        while ((1..20).any { n -> handles[n % handles.size].item("s-$n")?.state != ItemState.Running }) delay(200.milliseconds)
    }
    handles.first().detach()
    for (n in 1..20) handles.last().send("confirmed", "s-$n")
    withTimeout(3.minutes) { while (handled.size < 20) delay(200.milliseconds) }
    for (handle in handles.drop(1)) handle.detach()
    expect("$tag replicas handled every item once", handled.sorted() == (1..20).map { "s-$it" }.sorted()) { handled.sorted().toString() }
}

suspend fun scenarioModules(tag: String) {
    val dir = Files.createTempDirectory("loadshift-consumer")
    val registry = SimpleMeterRegistry()
    val logs = SqliteLogSink(dir.resolve("logs.db").toString())
    val store = SqliteDeadLetterStore(dir.resolve("dead.db").toString())
    val backend = LocalBackend()
    val console = ControlServer(backend, port = 18571).start()
    try {
        val config = RunConfig(logSink = logs, metrics = MicrometerMetrics(registry), tracer = OtelTracer(OpenTelemetry.noop()))
        scenarioOrders(backend, "$tag-modules", config, store)
        val response = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(URI("http://127.0.0.1:18571/api/runs")).build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        expect("$tag console lists runs", response.statusCode() == 200 && "orders-$tag-modules" in response.body()) { "${response.statusCode()} ${response.body().take(200)}" }
        expect("$tag metrics recorded", registry.meters.isNotEmpty()) { "no meters" }
    } finally {
        console.stop()
        logs.close()
        store.close()
    }
}

suspend fun scenarioEmbedded(tag: String) {
    val configuration = StandaloneInMemProcessEngineConfiguration()
    configuration.processEngineName = "consumer-$tag"
    configuration.jdbcUrl = "jdbc:h2:mem:consumer-$tag;DB_CLOSE_DELAY=-1"
    configuration.jdbcMaxActiveConnections = 32
    configuration.isJobExecutorActivate = true
    configuration.processEnginePlugins = mutableListOf(SpinProcessEnginePlugin())
    val engine = configuration.buildProcessEngine()
    try {
        val backend = Camunda7JavaApi(engine).backend()
        scenarioOrders(backend, "$tag-embedded")
        scenarioBatch(backend, "$tag-embedded", 300)
    } finally {
        engine.close()
    }
}

fun main(args: Array<String>): Unit = runBlocking {
    println("INFO java ${System.getProperty("java.version")}")
    val mode = args.firstOrNull() ?: "local"
    val tag = "${mode}-${System.currentTimeMillis() % 100000}"
    val backend: Backend = when (mode) {
        "local" -> LocalBackend()
        "c7" -> Camunda7Backend(args.getOrElse(1) { "http://localhost:8080/engine-rest" })
        "c8" -> args.getOrNull(3)?.let { grpc ->
            Camunda8Backend(args[1], jobStream = CamundaJobStream(URI(grpc), URI(args[1])))
        } ?: Camunda8Backend(args.getOrElse(1) { "http://localhost:8080" })
        "embedded" -> LocalBackend()
        else -> error("unknown mode $mode")
    }
    when (mode) {
        "local" -> {
            scenarioOrders(backend, tag)
            scenarioBatch(backend, tag, args.getOrElse(2) { "500" }.toInt())
            scenarioModules(tag)
        }
        "embedded" -> scenarioEmbedded(tag)
        else -> {
            scenarioOrders(backend, tag)
            scenarioBatch(backend, tag, args.getOrElse(2) { "500" }.toInt())
            scenarioRestart(backend, tag)
            val replica = when (mode) {
                "c7" -> Camunda7Backend(args[1])
                else -> Camunda8Backend(args[1])
            }
            scenarioReplicas(listOf(backend, replica), tag)
        }
    }
    println(if (failures.isEmpty()) "ALL PASSED ($mode)" else "FAILURES ($mode): $failures")
    kotlin.system.exitProcess(if (failures.isEmpty()) 0 else 1)
}
