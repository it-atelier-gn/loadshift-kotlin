package loadshift.core

import kotlinx.datetime.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

sealed interface Start {
    data object Now : Start
    data object Manual : Start
    data class At(val time: Instant) : Start
    data class Cron(val expr: String) : Start
}

data class Rate(val permits: Int, val per: Duration)

fun perSecond(permits: Int): Rate = Rate(permits, 1.seconds)

data class RetryPolicy(
    val maxAttempts: Int = 3,
    val baseDelay: Duration = 200.milliseconds,
    val maxDelay: Duration = 30.seconds,
    val jitter: Boolean = true,
    val retryOn: (Throwable) -> Boolean = { true },
    val timeout: Duration? = null,
) {
    companion object {
        val Default = RetryPolicy()
        val None = RetryPolicy(maxAttempts = 1)
    }
}

enum class ErrorPolicy { DeadLetter, Fail, Skip }

class TaskOptions(
    val retry: RetryPolicy? = null,
    val timeout: Duration? = null,
    val rateLimit: Rate? = null,
)

data class RunConfig(
    val start: Start = Start.Now,
    val maxConcurrency: Int = 16,
    val rateLimit: Rate? = null,
    val retry: RetryPolicy = RetryPolicy.Default,
    val onError: ErrorPolicy = ErrorPolicy.DeadLetter,
    val dryRun: Boolean = false,
    val dedupe: Boolean = false,
    val resume: Boolean = true,
    val lockDuration: Duration = 5.minutes,
    val maxLoopIterations: Int = 10_000,
    val logSink: LogSink = NoopLogSink,
)

data class Progress(
    val seeded: Long = 0,
    val expanded: Long = 0,
    val done: Long = 0,
    val failed: Long = 0,
    val skipped: Long = 0,
)

data class DeadLetter(val key: String?, val topic: String, val error: String)

data class RunResult(
    val done: Long,
    val failed: Long,
    val skipped: Long,
    val deadLetters: List<DeadLetter>,
)

interface RunHandle {
    suspend fun start()
    fun progress(): Progress
    suspend fun pause()
    suspend fun cancel()
    suspend fun await(): RunResult
    suspend fun send(message: String, key: String) {}
    suspend fun broadcast(message: String) {}
}
