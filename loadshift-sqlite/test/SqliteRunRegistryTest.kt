package loadshift.sqlite

import kotlinx.coroutines.test.runTest
import loadshift.core.Progress
import loadshift.core.RunRecord
import loadshift.core.RunState
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Instant

class SqliteRunRegistryTest {

    private fun tempPath() = Files.createTempFile("runs", ".db").toString()

    private fun record(id: String, startedMillis: Long, state: RunState = RunState.Running) = RunRecord(
        id = id,
        worker = "worker-a",
        backendType = "camunda8",
        workflowKey = "orders",
        workflowName = "Orders",
        state = state,
        startedAt = Instant.fromEpochMilliseconds(startedMillis),
        updatedAt = Instant.fromEpochMilliseconds(startedMillis + 10),
        staleAt = Instant.fromEpochMilliseconds(startedMillis + 20),
        progress = Progress(seeded = 6, expanded = 5, done = 4, failed = 3, skipped = 2, cancelled = 1),
        deadLetters = 7,
    )

    @Test
    fun savesReplacesAndReadsRecords() = runTest {
        SqliteRunRegistry(tempPath()).use { registry ->
            registry.save(record("w:run-1", 100))
            registry.save(record("w:run-1", 100, RunState.Completed))

            assertEquals(record("w:run-1", 100, RunState.Completed), registry.get("w:run-1"))
            assertNull(registry.get("missing"))
        }
    }

    @Test
    fun listsNewestFirstUpToTheLimitAndRemovesRecords() = runTest {
        SqliteRunRegistry(tempPath()).use { registry ->
            registry.save(record("w:run-1", 100))
            registry.save(record("w:run-3", 300))
            registry.save(record("w:run-2", 200))

            assertEquals(listOf("w:run-3", "w:run-2"), registry.list(limit = 2).map { it.id })
            registry.remove("w:run-3")
            assertEquals(listOf("w:run-2", "w:run-1"), registry.list().map { it.id })
            assertFailsWith<IllegalArgumentException> { registry.list(limit = 0) }
        }
    }

    @Test
    fun recordsSurviveReopen() = runTest {
        val path = tempPath()
        SqliteRunRegistry(path).use { it.save(record("w:run-1", 100)) }
        SqliteRunRegistry(path).use { assertEquals(record("w:run-1", 100), it.get("w:run-1")) }
    }
}
