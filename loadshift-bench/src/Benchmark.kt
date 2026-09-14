package loadshift.bench

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.serialization.Serializable
import loadshift.core.Backend
import loadshift.core.RunConfig
import loadshift.core.WorkItem
import loadshift.core.task
import loadshift.core.workflow
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.ceil
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.DurationUnit
import kotlin.time.TimeSource

@Serializable
data class BenchItem(val id: String) : WorkItem {
    override val key get() = id
}

class BenchmarkReport(
    val backend: String,
    val items: Int,
    val done: Long,
    val deadLetters: Int,
    val elapsed: Duration,
    latencies: List<Duration>,
) {
    val latencies: List<Duration> = latencies.sorted()

    val throughput: Double
        get() = if (elapsed.isPositive()) done / elapsed.toDouble(DurationUnit.SECONDS) else 0.0

    fun percentile(fraction: Double): Duration {
        require(fraction > 0.0 && fraction <= 1.0) { "fraction must be in (0, 1], was $fraction" }
        if (latencies.isEmpty()) return Duration.ZERO
        val rank = ceil(fraction * latencies.size).toInt().coerceIn(1, latencies.size)
        return latencies[rank - 1]
    }

    fun render(): String {
        fun millis(duration: Duration) = "%.1f".format(duration.toDouble(DurationUnit.MILLISECONDS))
        return buildString {
            appendLine("| Backend | Items | Done | Dead letters | Seconds | Items/s | p50 ms | p95 ms | p99 ms |")
            appendLine("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
            append("| $backend | $items | $done | $deadLetters | ")
            append("%.2f".format(elapsed.toDouble(DurationUnit.SECONDS)))
            append(" | ")
            append("%.1f".format(throughput))
            append(" | ${millis(percentile(0.50))} | ${millis(percentile(0.95))} | ${millis(percentile(0.99))} |")
        }
    }
}

data class BenchOptions(
    val backend: String = "local",
    val base: String? = null,
    val items: Int = 1000,
    val tasks: Int = 3,
    val concurrency: Int = 16,
    val work: Duration = Duration.ZERO,
) {
    init {
        require(backend in BACKENDS) { "backend must be one of ${BACKENDS.joinToString()}, was '$backend'" }
        require(items > 0) { "items must be positive, was $items" }
        require(tasks > 0) { "tasks must be positive, was $tasks" }
        require(concurrency > 0) { "concurrency must be positive, was $concurrency" }
        require(!work.isNegative()) { "work must not be negative, was $work" }
    }

    companion object {
        val BACKENDS = listOf("local", "camunda7", "camunda8")

        fun parse(args: List<String>): BenchOptions {
            require(args.size % 2 == 0) { "options are pairs of --name value" }
            var options = BenchOptions()
            for ((name, value) in args.chunked(2).map { it[0] to it[1] }) {
                fun number() = value.toIntOrNull() ?: throw IllegalArgumentException("$name expects a number, was '$value'")
                options = when (name) {
                    "--backend" -> options.copy(backend = value)
                    "--base" -> options.copy(base = value)
                    "--items" -> options.copy(items = number())
                    "--tasks" -> options.copy(tasks = number())
                    "--concurrency" -> options.copy(concurrency = number())
                    "--work" -> options.copy(work = number().milliseconds)
                    else -> throw IllegalArgumentException("unknown option '$name'")
                }
            }
            return options
        }
    }
}

suspend fun runBenchmark(
    backend: Backend,
    label: String,
    items: Int,
    tasks: Int,
    work: Duration,
    config: RunConfig,
): BenchmarkReport {
    require(items > 0) { "items must be positive, was $items" }
    require(tasks > 0) { "tasks must be positive, was $tasks" }
    val seeded = ConcurrentHashMap<String, TimeSource.Monotonic.ValueTimeMark>()
    val latencies = ConcurrentHashMap<String, Duration>()
    val benchmark = workflow<BenchItem>("bench-${System.nanoTime()}") {
        input {
            flow {
                repeat(items) { index ->
                    val item = BenchItem("item-$index")
                    seeded[item.id] = TimeSource.Monotonic.markNow()
                    emit(item)
                }
            }
        }
        repeat(tasks) { index ->
            task("step-$index") { item ->
                if (work.isPositive()) delay(work)
                if (index == tasks - 1) seeded[item.id]?.let { latencies[item.id] = it.elapsedNow() }
            }
        }
    }
    val started = TimeSource.Monotonic.markNow()
    val result = backend.run(benchmark, config).await()
    return BenchmarkReport(label, items, result.done, result.deadLetters.size, started.elapsedNow(), latencies.values.toList())
}
