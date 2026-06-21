package loadshift.sqlite

import kotlinx.coroutines.test.runTest
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SqliteCheckpointStoreTest {
    @Test
    fun marksAndReadsCompletionScopedByRunKey() = runTest {
        val path = Files.createTempFile("checkpoints", ".db").toString()
        val store = SqliteCheckpointStore(path)
        try {
            assertFalse(store.isComplete("wf", "a"))
            store.markComplete("wf", "a")
            store.markComplete("wf", "a")
            assertTrue(store.isComplete("wf", "a"))
            assertFalse(store.isComplete("wf", "b"))
            assertFalse(store.isComplete("other", "a"))
        } finally {
            store.close()
        }
    }

    @Test
    fun survivesReopen() = runTest {
        val path = Files.createTempFile("checkpoints", ".db").toString()
        SqliteCheckpointStore(path).use { it.markComplete("wf", "x") }
        SqliteCheckpointStore(path).use { assertTrue(it.isComplete("wf", "x")) }
    }
}
