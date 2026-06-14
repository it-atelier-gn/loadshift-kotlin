package loadshift.local

import kotlinx.coroutines.test.runTest
import loadshift.core.ErrorPolicy
import loadshift.core.RetryPolicy
import loadshift.core.RunConfig
import loadshift.core.RunState
import loadshift.core.WorkItemBase
import loadshift.core.workflow
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

private class Doc : WorkItemBase() {
    var id: String by required()
    override val key get() = id
}

private fun doc(id: String) = Doc().apply { this.id = id }

class LocalIntrospectionTest {

    @Test
    fun completedRunIsVisibleThroughIntrospection() = runTest {
        val backend = LocalBackend()
        val wf = workflow<Doc>("intro-ok") {
            input(listOf(doc("a"), doc("b")))
            task("noop") {}
        }
        backend.run(wf).await()

        val runs = backend.introspection.runs()
        assertEquals(1, runs.size)
        val snap = runs.single()
        assertEquals("local", backend.introspection.backendType)
        assertEquals("intro-ok", snap.workflowName)
        assertEquals(RunState.Completed, snap.state)
        assertEquals(2, snap.progress.seeded)
        assertEquals(2, snap.progress.done)
        assertEquals(emptyList(), snap.deadLetters)
        assertNotNull(backend.introspection.structure(snap.id))
        assertEquals("workflow", backend.introspection.structure(snap.id)?.type)
    }

    @Test
    fun deadLettersShowUpInSnapshot() = runTest {
        val backend = LocalBackend()
        val wf = workflow<Doc>("intro-dlq") {
            input(listOf(doc("bad")))
            task("explode", retry = RetryPolicy.None) { error("kaboom") }
        }
        backend.run(wf, RunConfig(onError = ErrorPolicy.DeadLetter)).await()

        val snap = backend.introspection.runs().single()
        assertEquals(RunState.Completed, snap.state)
        assertEquals(1, snap.progress.failed)
        assertEquals(1, snap.deadLetters.size)
        assertEquals("explode", snap.deadLetters.single().topic)
    }

    @Test
    fun eachRunGetsItsOwnSnapshot() = runTest {
        val backend = LocalBackend()
        val wf = workflow<Doc>("intro-multi") {
            input(listOf(doc("x")))
            task("noop") {}
        }
        backend.run(wf).await()
        backend.run(wf).await()
        assertEquals(2, backend.introspection.runs().size)
        assertEquals(2, backend.introspection.runs().map { it.id }.distinct().size)
    }
}
