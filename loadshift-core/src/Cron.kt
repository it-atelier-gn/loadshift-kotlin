package loadshift.core

import kotlinx.coroutines.delay
import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.isoDayNumber
import kotlinx.datetime.number
import kotlinx.datetime.plus
import kotlinx.datetime.toInstant
import kotlinx.datetime.toLocalDateTime
import kotlin.time.Clock
import kotlin.time.Instant

class CronExpression private constructor(
    val expr: String,
    private val minutes: List<Int>,
    private val hours: List<Int>,
    private val daysOfMonth: Set<Int>,
    private val months: Set<Int>,
    private val daysOfWeek: Set<Int>,
    private val dayOfMonthRestricted: Boolean,
    private val dayOfWeekRestricted: Boolean,
) {
    fun matches(date: LocalDate): Boolean {
        if (date.month.number !in months) return false
        val domMatch = date.day in daysOfMonth
        val dowMatch = date.dayOfWeek.isoDayNumber % 7 in daysOfWeek
        return if (dayOfMonthRestricted && dayOfWeekRestricted) domMatch || dowMatch else domMatch && dowMatch
    }

    fun next(after: Instant, zone: TimeZone): Instant {
        var date = after.toLocalDateTime(zone).date
        repeat(MAX_DAYS) {
            if (matches(date)) {
                for (hour in hours) for (minute in minutes) {
                    val candidate = LocalDateTime(date, LocalTime(hour, minute)).toInstant(zone)
                    if (candidate > after) return candidate
                }
            }
            date = date.plus(1, DateTimeUnit.DAY)
        }
        throw IllegalArgumentException("cron expression '$expr' never matches")
    }

    companion object {
        private const val MAX_DAYS = 8 * 366
        private val MONTH_NAMES = listOf("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
        private val DAY_NAMES = listOf("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT")

        fun parse(expr: String): CronExpression {
            val fields = expr.trim().split(Regex("\\s+"))
            require(fields.size == 5) { "cron expression must have 5 space-separated fields: '$expr'" }
            val months = parseField(expr, fields[3], 1, 12) { MONTH_NAMES.indexOf(it).takeIf { i -> i >= 0 }?.plus(1) }
            val daysOfMonth = parseField(expr, fields[2], 1, 31, null)
            val daysOfWeek = parseField(expr, fields[4], 0, 7) { DAY_NAMES.indexOf(it).takeIf { i -> i >= 0 } }
            return CronExpression(
                expr = expr,
                minutes = parseField(expr, fields[0], 0, 59, null).sorted(),
                hours = parseField(expr, fields[1], 0, 23, null).sorted(),
                daysOfMonth = daysOfMonth,
                months = months,
                daysOfWeek = daysOfWeek.map { it % 7 }.toSet(),
                dayOfMonthRestricted = !isWildcard(fields[2]),
                dayOfWeekRestricted = !isWildcard(fields[4]),
            )
        }

        private fun isWildcard(field: String) = field == "*" || field == "?"

        private fun parseField(
            expr: String,
            field: String,
            min: Int,
            max: Int,
            name: ((String) -> Int?)?,
        ): Set<Int> {
            fun value(token: String): Int {
                val v = token.toIntOrNull() ?: name?.invoke(token.uppercase())
                    ?: throw IllegalArgumentException("invalid value '$token' in cron expression '$expr'")
                require(v in min..max) { "value $v out of range $min-$max in cron expression '$expr'" }
                return v
            }

            val result = mutableSetOf<Int>()
            for (part in field.split(",")) {
                require(part.isNotEmpty()) { "empty list entry in cron expression '$expr'" }
                val segments = part.split("/")
                require(segments.size <= 2) { "invalid step '$part' in cron expression '$expr'" }
                val range = segments[0]
                val step = segments.getOrNull(1)?.let {
                    it.toIntOrNull()?.takeIf { s -> s > 0 }
                        ?: throw IllegalArgumentException("invalid step '$it' in cron expression '$expr'")
                }
                val (start, end) = when {
                    isWildcard(range) -> min to max
                    "-" in range -> range.split("-", limit = 2).let { value(it[0]) to value(it[1]) }
                    step != null -> value(range) to max
                    else -> value(range).let { it to it }
                }
                require(start <= end) { "range $start-$end is reversed in cron expression '$expr'" }
                var v = start
                while (v <= end) {
                    result += v
                    v += step ?: 1
                }
            }
            return result
        }
    }
}

object CronSchedule {
    fun next(expr: String, after: Instant, zone: TimeZone = TimeZone.currentSystemDefault()): Instant =
        CronExpression.parse(expr).next(after, zone)
}

suspend fun CronSchedule.awaitNext(expr: String, zone: TimeZone = TimeZone.currentSystemDefault()) {
    val target = next(expr, Clock.System.now(), zone)
    val waitMs = target.toEpochMilliseconds() - Clock.System.now().toEpochMilliseconds()
    if (waitMs > 0) delay(waitMs)
}
