package loadshift.micrometer

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.test.runTest
import loadshift.core.MetricNames
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.WorkItem
import loadshift.core.task
import loadshift.core.workflow
import loadshift.local.LocalBackend
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds

private data class Parcel(val id: String) : WorkItem {
    override val key get() = id
}

class MicrometerMetricsTest {

    @Test
    fun countersAndTimersCarryTheAttributesAsTags() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerMetrics(registry)

        metrics.increment("loadshift.items", mapOf("workflow" to "ship", "state" to "done"), 3)
        metrics.record("loadshift.task.duration", mapOf("workflow" to "ship", "topic" to "pack"), 250.milliseconds)

        assertEquals(3.0, registry.get("loadshift.items").tags("workflow", "ship", "state", "done").counter().count())
        val timer = registry.get("loadshift.task.duration").tags("topic", "pack").timer()
        assertEquals(1, timer.count())
        assertEquals(250.0, timer.totalTime(TimeUnit.MILLISECONDS))
    }

    @Test
    fun aLocalRunReportsItemsTaskDurationsRetriesAndDeadLetters() = runTest {
        val registry = SimpleMeterRegistry()
        val wf = workflow<Parcel>("parcels") {
            input(listOf(Parcel("ok"), Parcel("bad")))
            task("pack", retry = RetryPolicy(maxAttempts = 2, baseDelay = 1.milliseconds, jitter = false)) {
                if (it.id == "bad") error("torn")
            }
        }

        LocalBackend().run(wf, RunConfig(metrics = MicrometerMetrics(registry))).await()

        fun items(state: String) = registry.get(MetricNames.ITEMS).tags("workflow", "parcels", "state", state).counter().count()
        assertEquals(2.0, items("seeded"))
        assertEquals(1.0, items("done"))
        assertEquals(1.0, registry.get(MetricNames.TASK_RETRIES).tags("topic", "pack").counter().count())
        assertEquals(1.0, registry.get(MetricNames.DEAD_LETTERS).tags("topic", "pack").counter().count())
        assertEquals(1, registry.get(MetricNames.TASK_DURATION).tags("outcome", "success").timer().count())
        assertEquals(2, registry.get(MetricNames.TASK_DURATION).tags("outcome", "failure").timer().count())
    }
}
