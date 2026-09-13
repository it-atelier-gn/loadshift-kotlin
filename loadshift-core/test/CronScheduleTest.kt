package loadshift.core

import kotlinx.datetime.TimeZone
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Instant

class CronScheduleTest {
    private val utc = TimeZone.UTC

    private fun next(expr: String, after: String, zone: TimeZone = utc): Instant =
        CronSchedule.next(expr, Instant.parse(after), zone)

    @Test
    fun everyMinuteAdvancesToNextMinute() {
        assertEquals(Instant.parse("2026-06-15T10:01:00Z"), next("* * * * *", "2026-06-15T10:00:30Z"))
    }

    @Test
    fun exactMinuteBoundaryMovesToFollowingMinute() {
        assertEquals(Instant.parse("2026-06-15T10:01:00Z"), next("* * * * *", "2026-06-15T10:00:00Z"))
    }

    @Test
    fun topOfEveryHour() {
        assertEquals(Instant.parse("2026-06-15T11:00:00Z"), next("0 * * * *", "2026-06-15T10:01:00Z"))
    }

    @Test
    fun everyFifteenMinutes() {
        assertEquals(Instant.parse("2026-06-15T10:30:00Z"), next("*/15 * * * *", "2026-06-15T10:16:00Z"))
    }

    @Test
    fun stepFromStartValueRunsToEndOfRange() {
        assertEquals(Instant.parse("2026-06-15T10:25:00Z"), next("5/20 * * * *", "2026-06-15T10:06:00Z"))
    }

    @Test
    fun steppedRange() {
        assertEquals(Instant.parse("2026-06-15T12:00:00Z"), next("0 8-18/4 * * *", "2026-06-15T08:30:00Z"))
    }

    @Test
    fun listsAndRangesCombine() {
        assertEquals(Instant.parse("2026-06-15T14:00:00Z"), next("0 8-10,14 * * *", "2026-06-15T10:30:00Z"))
    }

    @Test
    fun nextMondayAtNine() {
        assertEquals(Instant.parse("2026-06-22T09:00:00Z"), next("0 9 * * 1", "2026-06-17T12:00:00Z"))
    }

    @Test
    fun dailyAtFixedTimeSkipsToTomorrowOnceTimePassed() {
        assertEquals(Instant.parse("2026-06-16T09:00:00Z"), next("0 9 * * *", "2026-06-15T23:30:00Z"))
    }

    @Test
    fun sevenAndZeroBothMeanSunday() {
        assertEquals(Instant.parse("2026-06-21T00:00:00Z"), next("0 0 * * 7", "2026-06-17T12:00:00Z"))
        assertEquals(Instant.parse("2026-06-21T00:00:00Z"), next("0 0 * * 0", "2026-06-17T12:00:00Z"))
    }

    @Test
    fun dayOfMonthOrDayOfWeekWhenBothAreRestricted() {
        assertEquals(Instant.parse("2026-06-22T09:00:00Z"), next("0 9 1 * MON", "2026-06-17T12:00:00Z"))
        assertEquals(Instant.parse("2026-07-01T09:00:00Z"), next("0 9 1 * MON", "2026-06-29T10:00:00Z"))
    }

    @Test
    fun dayOfMonthAloneIgnoresWeekday() {
        assertEquals(Instant.parse("2026-07-01T09:00:00Z"), next("0 9 1 * *", "2026-06-17T12:00:00Z"))
        assertEquals(Instant.parse("2026-07-01T09:00:00Z"), next("0 9 1 * ?", "2026-06-17T12:00:00Z"))
    }

    @Test
    fun monthAndDayNamesAreCaseInsensitive() {
        assertEquals(Instant.parse("2027-01-03T00:00:00Z"), next("0 0 * jan Sun", "2026-06-15T00:00:00Z"))
        assertEquals(Instant.parse("2026-06-19T17:00:00Z"), next("0 17 * * MON-FRI", "2026-06-19T16:00:00Z"))
    }

    @Test
    fun leapDayIsFoundYearsAhead() {
        assertEquals(Instant.parse("2028-02-29T00:00:00Z"), next("0 0 29 2 *", "2026-06-15T00:00:00Z"))
    }

    @Test
    fun evaluatesInTheGivenZone() {
        val berlin = TimeZone.of("Europe/Berlin")
        assertEquals(Instant.parse("2026-06-15T07:00:00Z"), next("0 9 * * *", "2026-06-15T06:00:00Z", berlin))
    }

    @Test
    fun rejectsMalformedExpressions() {
        for (bad in listOf("* * * *", "60 * * * *", "*/0 * * * *", "5-1 * * * *", "0 0 * FOO *", "0 0 31 2 *", "a b c d e", "1,,2 * * * *")) {
            assertFailsWith<IllegalArgumentException>(bad) { next(bad, "2026-06-15T00:00:00Z") }
        }
    }

    @Test
    fun cronStartValidatesExpressionEagerly() {
        assertFailsWith<IllegalArgumentException> { Start.Cron("not a cron") }
        assertEquals("0 9 * * *", Start.Cron("0 9 * * *", utc).expr)
    }
}
