package loadshift.local

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import loadshift.core.ErrorPolicy
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.RunState
import loadshift.core.WorkItem
import loadshift.core.workflow
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@Serializable
private data class Doc(var id: String) : WorkItem {
    override val key get() = id
}

class LocalControlTest {

    @Test
    fun completedRunIsVisibleThroughControl() = runTest {
        val backend = LocalBackend()
        val wf = workflow<Doc>("intro-ok") {
            input(listOf(Doc("a"), Doc("b")))
            task("noop") {}
        }
        backend.run(wf).await()

        val runs = backend.control.runs()
        assertEquals(1, runs.size)
        val snap = runs.single()
        assertEquals("local", backend.control.backendType)
        assertEquals("intro-ok", snap.workflowName)
        assertEquals(RunState.Completed, snap.state)
        assertEquals(2, snap.progress.seeded)
        assertEquals(2, snap.progress.done)
        assertEquals(emptyList(), snap.deadLetters)
        assertNotNull(backend.control.structure(snap.id))
        assertEquals("workflow", backend.control.structure(snap.id)?.type)
    }

    @Test
    fun deadLettersShowUpInSnapshot() = runTest {
        val backend = LocalBackend()
        val wf = workflow<Doc>("intro-dlq") {
            input(listOf(Doc("bad")))
            task("explode", retry = RetryPolicy.None) { error("kaboom") }
        }
        backend.run(wf, RunConfig(onError = ErrorPolicy.DeadLetter)).await()

        val snap = backend.control.runs().single()
        assertEquals(RunState.Completed, snap.state)
        assertEquals(0, snap.progress.failed)
        assertEquals(1, snap.deadLetters.size)
        assertEquals("explode", snap.deadLetters.single().topic)
    }

    @Test
    fun eachRunGetsItsOwnSnapshot() = runTest {
        val backend = LocalBackend()
        val wf = workflow<Doc>("intro-multi") {
            input(listOf(Doc("x")))
            task("noop") {}
        }
        backend.run(wf).await()
        backend.run(wf).await()
        assertEquals(2, backend.control.runs().size)
        assertEquals(2, backend.control.runs().map { it.id }.distinct().size)
    }
}
