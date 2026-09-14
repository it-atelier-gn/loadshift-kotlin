package loadshift.bench

import kotlinx.coroutines.runBlocking
import loadshift.core.RunConfig
import loadshift.local.LocalBackend
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class BenchmarkTest {

    @Test
    fun optionsAreParsedAndValidated() {
        assertEquals(BenchOptions(), BenchOptions.parse(emptyList()))
        assertEquals(
            BenchOptions("camunda8", "http://engine:8080", 50, 2, 8, 5.milliseconds),
            BenchOptions.parse(
                listOf("--backend", "camunda8", "--base", "http://engine:8080", "--items", "50", "--tasks", "2", "--concurrency", "8", "--work", "5"),
            ),
        )
        assertFailsWith<IllegalArgumentException> { BenchOptions.parse(listOf("--items")) }
        assertFailsWith<IllegalArgumentException> { BenchOptions.parse(listOf("--speed", "1")) }
        assertFailsWith<IllegalArgumentException> { BenchOptions.parse(listOf("--items", "many")) }
        assertFailsWith<IllegalArgumentException> { BenchOptions.parse(listOf("--backend", "other")) }
        assertFailsWith<IllegalArgumentException> { BenchOptions.parse(listOf("--items", "0")) }
    }

    @Test
    fun percentilesUseTheNearestRank() {
        val report = BenchmarkReport("local", 4, 4, 0, 2.seconds, listOf(40, 10, 30, 20).map { it.milliseconds })

        assertEquals(10.milliseconds, report.percentile(0.25))
        assertEquals(20.milliseconds, report.percentile(0.50))
        assertEquals(40.milliseconds, report.percentile(0.99))
        assertEquals(2.0, report.throughput)
        assertEquals(Duration.ZERO, BenchmarkReport("local", 1, 0, 0, Duration.ZERO, emptyList()).percentile(0.5))
        assertFailsWith<IllegalArgumentException> { report.percentile(0.0) }
    }

    @Test
    fun aLocalRunReportsEveryItemWithItsLatency() = runBlocking {
        val report = runBenchmark(LocalBackend(), "local", items = 40, tasks = 2, work = 1.milliseconds, config = RunConfig(maxConcurrency = 8))

        assertEquals(40, report.done)
        assertEquals(40, report.latencies.size)
        assertTrue(report.percentile(0.5) <= report.percentile(0.99))
        assertTrue(report.throughput > 0.0)
        val table = report.render().lines()
        assertEquals("| Backend | Items | Done | Dead letters | Seconds | Items/s | p50 ms | p95 ms | p99 ms |", table.first())
        assertTrue(table.last().startsWith("| local | 40 | 40 | 0 | "), table.last())
    }
}
