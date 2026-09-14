package loadshift.core

import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration

interface Metrics {
    fun increment(name: String, attributes: Map<String, String>, amount: Long = 1)
    fun record(name: String, attributes: Map<String, String>, duration: Duration)
}

object NoopMetrics : Metrics {
    override fun increment(name: String, attributes: Map<String, String>, amount: Long) {}
    override fun record(name: String, attributes: Map<String, String>, duration: Duration) {}
}

object MetricNames {
    const val TASK_DURATION = "loadshift.task.duration"
    const val TASK_RETRIES = "loadshift.task.retries"
    const val DEAD_LETTERS = "loadshift.dead.letters"
    const val ITEMS = "loadshift.items"
    const val LOCK_EXTENSIONS = "loadshift.lock.extensions"

    const val WORKFLOW = "workflow"
    const val TOPIC = "topic"
    const val OUTCOME = "outcome"
    const val STATE = "state"

    const val SUCCESS = "success"
    const val FAILURE = "failure"
}

class RunMetrics(private val metrics: Metrics, private val workflowKey: String) {
    private val seeded = AtomicLong()
    private val expanded = AtomicLong()
    private val done = AtomicLong()
    private val failed = AtomicLong()
    private val skipped = AtomicLong()
    private val cancelled = AtomicLong()

    fun seeded(amount: Long = 1) = count(seeded, "seeded", amount)
    fun expanded(amount: Long = 1) = count(expanded, "expanded", amount)
    fun done() = count(done, "done", 1)
    fun failed() = count(failed, "failed", 1)
    fun skipped() = count(skipped, "skipped", 1)
    fun cancelled() = count(cancelled, "cancelled", 1)

    fun progress(): Progress =
        Progress(seeded.get(), expanded.get(), done.get(), failed.get(), skipped.get(), cancelled.get())

    fun result(deadLetters: List<DeadLetter>): RunResult =
        RunResult(done.get(), failed.get(), skipped.get(), deadLetters, cancelled.get())

    fun taskDuration(topic: String, success: Boolean, duration: Duration) =
        metrics.record(MetricNames.TASK_DURATION, attributes(topic, success), duration)

    fun retry(topic: String) = metrics.increment(MetricNames.TASK_RETRIES, attributes(topic))

    fun deadLetter(topic: String) = metrics.increment(MetricNames.DEAD_LETTERS, attributes(topic))

    fun lockExtension(topic: String, success: Boolean) =
        metrics.increment(MetricNames.LOCK_EXTENSIONS, attributes(topic, success))

    private fun count(counter: AtomicLong, state: String, amount: Long) {
        if (amount <= 0) return
        counter.addAndGet(amount)
        metrics.increment(MetricNames.ITEMS, mapOf(MetricNames.WORKFLOW to workflowKey, MetricNames.STATE to state), amount)
    }

    private fun attributes(topic: String) = mapOf(MetricNames.WORKFLOW to workflowKey, MetricNames.TOPIC to topic)

    private fun attributes(topic: String, success: Boolean) = mapOf(
        MetricNames.WORKFLOW to workflowKey,
        MetricNames.TOPIC to topic,
        MetricNames.OUTCOME to if (success) MetricNames.SUCCESS else MetricNames.FAILURE,
    )
}
