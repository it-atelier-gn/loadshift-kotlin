package loadshift.core

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.time.Duration

@Serializable
private data class Crate(val id: String) : WorkItem {
    override val key get() = id
}

private class RecordingMetrics : Metrics {
    val counts = ConcurrentHashMap<Pair<String, Map<String, String>>, Long>()
    val durations = Collections.synchronizedList(mutableListOf<Pair<String, Map<String, String>>>())

    override fun increment(name: String, attributes: Map<String, String>, amount: Long) {
        counts.merge(name to attributes, amount, Long::plus)
    }

    override fun record(name: String, attributes: Map<String, String>, duration: Duration) {
        durations += name to attributes
    }
}

class MetricsTest {

    @Test
    fun counterChangesAreReportedAsItemsWithTheCounterAsState() {
        val recorder = RecordingMetrics()
        val metrics = RunMetrics(recorder, "ship")

        metrics.seeded(3)
        metrics.done()
        metrics.expanded(0)

        assertEquals(Progress(seeded = 3, done = 1), metrics.progress())
        assertEquals(
            mapOf(
                (MetricNames.ITEMS to mapOf("workflow" to "ship", "state" to "seeded")) to 3L,
                (MetricNames.ITEMS to mapOf("workflow" to "ship", "state" to "done")) to 1L,
            ),
            recorder.counts.toMap(),
        )
    }

    @OptIn(EngineApi::class)
    @Test
    fun engineJobsReportDurationsRetriesDeadLettersAndLockExtensions() = runTest {
        val recorder = RecordingMetrics()
        val wf = workflow<Crate>("crates") {
            input(emptyList())
            task("pack", retry = RetryPolicy(maxAttempts = 2)) { error("torn") }
        }
        val run = EngineRun(wf, RunConfig(metrics = recorder))
        val type = EngineNames.jobType(wf.key, "pack")
        val variables = run.rootVariables(Crate("c-1"))

        assertIs<JobOutcome.Retry>(run.execute(EngineJob("j-1", type, "pi-1", variables, 2)))
        assertIs<JobOutcome.Terminate>(run.execute(EngineJob("j-2", type, "pi-1", variables, 1)))
        run.recordLockExtension(EngineJob("j-3", type, "pi-1", variables, 1), success = false)

        val pack = mapOf("workflow" to wf.key, "topic" to "pack")
        assertEquals(1L, recorder.counts[MetricNames.TASK_RETRIES to pack])
        assertEquals(1L, recorder.counts[MetricNames.DEAD_LETTERS to pack])
        assertEquals(1L, recorder.counts[MetricNames.LOCK_EXTENSIONS to pack + ("outcome" to "failure")])
        assertEquals(List(2) { MetricNames.TASK_DURATION to pack + ("outcome" to "failure") }, recorder.durations.toList())
    }
}
