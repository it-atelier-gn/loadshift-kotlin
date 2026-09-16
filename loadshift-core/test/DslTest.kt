package loadshift.core

import kotlinx.serialization.Serializable
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

@Serializable
private data class Item(var n: Int) : WorkItem {
    override val key get() = n.toString()
}

@Serializable
private data class Child(var v: Int) : WorkItem

private fun richWorkflow(): Workflow<Item> = workflow("wf") {
    input(Item(1))
    task("a") { }
    condition({ it.n > 0 }) { task("b") { } } otherwise { task("c") { } }
    loop({ it.n < 3 }) { task("d") { } }
    parallel {
        branch { task("e") { } }
        branch { task("f") { } }
    }
    fanOut(expand = { listOf(Child(0)) }) {
        task("g") { }
    }
}

class DslTest {
    @Test
    fun buildsExpectedTopLevelStructure() {
        val wf = richWorkflow()
        val seq = assertIs<Sequence<Item>>(wf.root.step)
        assertIs<Execute<Item>>(seq.steps[0])
        assertIs<Conditional<Item>>(seq.steps[1])
        assertIs<Loop<Item>>(seq.steps[2])
        assertIs<Parallel<Item>>(seq.steps[3])
        assertIs<FanOut<Item, *>>(seq.steps[4])
    }

    @Test
    fun registersTasksAndDecisionsAtCorrectLevel() {
        val wf = richWorkflow()
        assertTrue(wf.root.tasks.keys.containsAll(listOf("a", "b", "c", "d", "e", "f")))
        assertTrue("g" !in wf.root.tasks.keys)
        assertEquals(2, wf.root.decisions.size)
    }

    @Test
    fun conditionalCarriesBothBranches() {
        val wf = richWorkflow()
        val seq = wf.root.step as Sequence<Item>
        val cond = seq.steps[1] as Conditional<Item>
        assertTrue(cond.onFalse != null)
        assertTrue(cond.predicate(Item(5)))
    }

    @Test
    fun fanOutChildIsSeparateLevel() {
        val wf = richWorkflow()
        val seq = wf.root.step as Sequence<Item>
        val fanOut = seq.steps[4] as FanOut<Item, *>
        assertTrue("g" in fanOut.body.tasks.keys)
    }

    @Test
    fun fanOutReduceBuildsFanInStepWithSeparateChildLevel() {
        val wf = workflow<Item>("fan-in") {
            input(Item(1))
            fanOut(expand = { listOf(Child(2), Child(3)) }) {
                task("h") { }
            }.reduce(0, combine = { acc, c -> acc + c.v }) { item, total -> item.n = total }
        }
        val seq = wf.root.step as Sequence<Item>
        val fanIn = assertIs<FanIn<Item, *, *>>(seq.steps[0])
        assertTrue("h" in fanIn.body.tasks.keys)
        assertTrue("h" !in wf.root.tasks.keys)
    }

    @Test
    fun waitBuildsWaitStepWithDuration() {
        val wf = workflow<Item>("waiter") {
            input(Item(1))
            wait(5.minutes)
        }
        val seq = wf.root.step as Sequence<Item>
        val w = assertIs<Wait<Item>>(seq.steps[0])
        assertEquals(5.minutes, w.duration)
    }

    @Test
    fun timeoutWrapsBodyInTimeoutStep() {
        val wf = workflow<Item>("to") {
            input(Item(1))
            timeout(30.seconds) { task("inner") { } }
        }
        val seq = wf.root.step as Sequence<Item>
        val to = assertIs<Timeout<Item>>(seq.steps[0])
        assertEquals(30.seconds, to.duration)
        assertTrue("inner" in wf.root.tasks.keys)
    }

    @Test
    fun rejectsDuplicateTopicsAcrossLevels() {
        val error = assertFailsWith<IllegalArgumentException> {
            workflow<Item>("dup") {
                input(Item(1))
                task("a") { }
                fanOut(expand = { listOf(Child(0)) }) {
                    task("a") { }
                }
            }
        }
        assertTrue("'a'" in error.message.orEmpty())
    }

    @Test
    fun rejectsTopicsReservedForGeneratedSteps() {
        for (topic in listOf("decision_c1", "decision_l2", "expand_f3", "reduce_f4", "timeout_to5", "loop_l6", "message_msg7")) {
            assertFailsWith<IllegalArgumentException>(topic) {
                workflow<Item>("reserved") {
                    input(Item(1))
                    task(topic) { }
                }
            }
        }
        workflow<Item>("allowed") {
            input(Item(1))
            task("expand_catalog") { }
        }
    }

    @Test
    fun rejectsBlankTopics() {
        assertFailsWith<IllegalArgumentException> {
            workflow<Item>("blank") {
                input(Item(1))
                task(" ") { }
            }
        }
    }

    @Test
    fun rejectsWorkflowNamesThatCannotFormProcessIds() {
        assertFailsWith<IllegalArgumentException> { workflow<Item>("") { input(Item(1)) } }
        assertFailsWith<IllegalArgumentException> { workflow<Item>("1st-run") { input(Item(1)) } }
        assertEquals("order-sync", workflow<Item>("Order Sync") { input(Item(1)) }.key)
    }

    @Test
    fun nestedLevelsShareOneIdSequence() {
        val wf = workflow<Item>("nested-ids") {
            input(Item(1))
            condition({ true }) { task("root-yes") { } }
            fanOut(expand = { listOf(Child(0)) }) {
                condition({ true }) { task("child-yes") { } }
                fanOut(expand = { listOf(Item(0)) }) {
                    condition({ true }) { task("grandchild-yes") { } }
                }
            }
        }
        val ids = mutableListOf<String>()
        fun visit(step: Step<*>) {
            when (step) {
                is Sequence<*> -> step.steps.forEach(::visit)
                is Conditional<*> -> {
                    ids += step.id
                    visit(step.onTrue)
                }
                is FanOut<*, *> -> {
                    ids += step.id
                    visit(step.body.step)
                }
                else -> Unit
            }
        }
        visit(wf.root.step)
        assertEquals(5, ids.size)
        assertEquals(ids.size, ids.toSet().size)
    }

    @Test
    fun awaitMessageBuildsAwaitMessageStep() {
        val wf = workflow<Item>("await") {
            input(Item(1))
            awaitMessage("payment-confirmed")
        }
        val seq = wf.root.step as Sequence<Item>
        val m = assertIs<AwaitMessage<Item>>(seq.steps[0])
        assertEquals("payment-confirmed", m.message)
    }

    @Test
    fun catchingAddsBranchesToTheTaskInDeclarationOrder() {
        val wf = workflow<Item>("catches") {
            input(Item(1))
            task("reserve") { }
                .catching<IllegalStateException> { task("retry-later") { } }
                .catching(IllegalArgumentException::class) { }
                .compensate { }
        }
        val execute = assertIs<Execute<Item>>(assertIs<Sequence<Item>>(wf.root.step).steps.single())
        assertEquals(listOf(IllegalStateException::class, IllegalArgumentException::class), execute.catches.map { it.type })
        assertEquals(listOf("ce1", "ce2"), execute.catches.map { it.id })
        assertTrue(execute.compensation != null)
        assertTrue("retry-later" in wf.root.tasks)
    }

    @Test
    fun topicsInsideCatchAndTimeoutBranchesMustBeUnique() {
        assertFailsWith<IllegalArgumentException> {
            workflow<Item>("dup-catch") {
                task("a") { }.catching<IllegalStateException> { task("a") { } }
            }
        }
        assertFailsWith<IllegalArgumentException> {
            workflow<Item>("dup-timeout") {
                task("a") { }
                awaitMessage("m", timeout = 1.seconds) onTimeout { task("a") { } }
            }
        }
    }

    @Test
    fun awaitMessageKeepsItsTimeoutHandlerAndTimeoutBranch() {
        val wf = workflow<Item>("messages") {
            awaitMessage("paid", timeout = 5.minutes) { item, _ -> item.n++ } onTimeout { task("remind") { } }
        }
        val await = assertIs<AwaitMessage<Item>>(assertIs<Sequence<Item>>(wf.root.step).steps.single())
        assertEquals(5.minutes, await.timeout)
        assertTrue(await.onMessage != null)
        assertIs<Sequence<Item>>(await.onTimeout)

        assertFailsWith<IllegalArgumentException> { workflow<Item>("no-timeout") { awaitMessage("paid") onTimeout { } } }
        assertFailsWith<IllegalArgumentException> { workflow<Item>("zero") { awaitMessage("paid", timeout = 0.seconds) } }
        assertFailsWith<IllegalArgumentException> { workflow<Item>("blank") { awaitMessage(" ") } }
    }
}
