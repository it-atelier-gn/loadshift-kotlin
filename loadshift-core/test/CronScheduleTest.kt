package loadshift.core

import kotlinx.datetime.Instant
import kotlinx.datetime.TimeZone
import kotlin.test.Test
import kotlin.test.assertEquals

class CronScheduleTest {
    private val utc = TimeZone.UTC

    @Test
    fun everyMinuteAdvancesToNextMinute() {
        val after = Instant.parse("2026-06-15T10:00:30Z")
        val next = CronSchedule.next("* * * * *", after, utc)
        assertEquals(Instant.parse("2026-06-15T10:01:00Z"), next)
    }

    @Test
    fun topOfEveryHour() {
        val after = Instant.parse("2026-06-15T10:01:00Z")
        val next = CronSchedule.next("0 * * * *", after, utc)
        assertEquals(Instant.parse("2026-06-15T11:00:00Z"), next)
    }

    @Test
    fun everyFifteenMinutes() {
        val after = Instant.parse("2026-06-15T10:16:00Z")
        val next = CronSchedule.next("*/15 * * * *", after, utc)
        assertEquals(Instant.parse("2026-06-15T10:30:00Z"), next)
    }

    @Test
    fun nextMondayAtNine() {
        val after = Instant.parse("2026-06-17T12:00:00Z") // Wednesday
        val next = CronSchedule.next("0 9 * * 1", after, utc)
        assertEquals(Instant.parse("2026-06-22T09:00:00Z"), next) // following Monday
    }

    @Test
    fun dailyAtFixedTimeSkipsToTomorrowOnceTimePassed() {
        val after = Instant.parse("2026-06-15T23:30:00Z")
        val next = CronSchedule.next("0 9 * * *", after, utc)
        assertEquals(Instant.parse("2026-06-16T09:00:00Z"), next)
    }
}
