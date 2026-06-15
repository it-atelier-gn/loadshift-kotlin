package loadshift.core

import kotlinx.coroutines.delay
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toInstant
import kotlinx.datetime.toLocalDateTime
import kotlin.time.Duration.Companion.minutes

object CronSchedule {
    private const val MAX_TICKS = 4 * 366 * 24 * 60

    fun next(expr: String, after: Instant, zone: TimeZone = TimeZone.currentSystemDefault()): Instant {
        val fields = expr.trim().split(Regex("\\s+"))
        require(fields.size == 5) { "cron expression must have 5 space-separated fields: '$expr'" }

        val minutes = parseField(fields[0], 0, 59)
        val hours = parseField(fields[1], 0, 23)
        val daysOfMonth = parseField(fields[2], 1, 31)
        val months = parseField(fields[3], 1, 12)
        val daysOfWeek = parseField(fields[4], 0, 7).map { it % 7 }.toSet()

        var candidate = truncateToMinute(after + 1.minutes, zone)
        repeat(MAX_TICKS) {
            val dt = candidate.toLocalDateTime(zone)
            val dow = (dt.dayOfWeek.ordinal + 1) % 7
            if (dt.minute in minutes && dt.hour in hours && dt.dayOfMonth in daysOfMonth &&
                dt.monthNumber in months && dow in daysOfWeek
            ) {
                return candidate
            }
            candidate += 1.minutes
        }
        error("no matching time found for cron expression '$expr'")
    }

    private fun truncateToMinute(instant: Instant, zone: TimeZone): Instant {
        val dt = instant.toLocalDateTime(zone)
        return LocalDateTime(dt.year, dt.monthNumber, dt.dayOfMonth, dt.hour, dt.minute).toInstant(zone)
    }

    private fun parseField(field: String, min: Int, max: Int): Set<Int> {
        val result = mutableSetOf<Int>()
        for (part in field.split(",")) {
            val segments = part.split("/")
            val range = segments[0]
            val step = segments.getOrNull(1)?.toInt() ?: 1
            val (start, end) = when {
                range == "*" -> min to max
                "-" in range -> range.split("-").let { it[0].toInt() to it[1].toInt() }
                else -> range.toInt().let { it to it }
            }
            var v = start
            while (v <= end) {
                result += v
                v += step
            }
        }
        return result
    }
}

suspend fun CronSchedule.awaitNext(expr: String, zone: TimeZone = TimeZone.currentSystemDefault()) {
    val target = next(expr, Clock.System.now(), zone)
    val waitMs = target.toEpochMilliseconds() - Clock.System.now().toEpochMilliseconds()
    if (waitMs > 0) delay(waitMs)
}
