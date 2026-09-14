package loadshift.core

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Instant

@Serializable
private data class LevelOrder(var id: String) : WorkItem

@Serializable
private data class LevelLine(var sku: String) : WorkItem

class WorkflowLevelsTest {

    private val wf = workflow<LevelOrder>("levels") {
        input(emptyList())
        condition({ true }) {
            fanOut(expand = { emptyList<LevelLine>() }) {
                timeout(kotlin.time.Duration.parse("1s")) {
                    fanOut(expand = { emptyList<LevelOrder>() }) {
                        task("deep") { }
                    }
                }
            }
        }
    }

    private fun record(level: String, itemVariable: String?, workflowKey: String = wf.key, id: String = "r1") = DeadLetterRecord(
        id = id,
        workflowKey = workflowKey,
        level = level,
        itemVariable = itemVariable,
        deadLetter = DeadLetter("k", "deep", "boom"),
        item = JsonObject(emptyMap()),
        recordedAt = Instant.fromEpochMilliseconds(0),
    )

    @Test
    fun listsEveryLevelWithItemVariableAndAncestorCodecs() {
        val levels = wf.levels().values.toList()

        assertEquals(3, levels.size)
        assertEquals(wf.key, levels[0].key)
        assertNull(levels[0].itemVariable)
        assertEquals(0, levels[0].ancestorCodecs.size)
        assertEquals(listOf(1, 2), levels.drop(1).map { it.ancestorCodecs.size })
        levels.drop(1).forEach { level ->
            val fanId = level.key.substringAfterLast('_')
            assertEquals(EngineNames.item(fanId), level.itemVariable)
        }
    }

    @Test
    fun acceptsRecordsOfTheWorkflowLevels() {
        val child = wf.levels().values.last()
        val accepted = wf.requireRequeueable(
            listOf(record(wf.key, null), record(child.key, child.itemVariable, id = "r2")),
            RunConfig(),
        )
        assertEquals(wf.levels().keys, accepted.keys)
    }

    @Test
    fun rejectsRecordsThatDoNotMatchTheWorkflow() {
        val child = wf.levels().values.last()
        val invalid = listOf(
            listOf(record(wf.key, null, workflowKey = "other")),
            listOf(record("levels_f99", "f99_item")),
            listOf(record(child.key, null)),
            listOf(record(wf.key, null), record(wf.key, null)),
        )
        for (records in invalid) {
            assertFailsWith<IllegalArgumentException> { wf.requireRequeueable(records, RunConfig()) }
        }
        assertFailsWith<IllegalArgumentException> {
            wf.requireRequeueable(listOf(record(wf.key, null)), RunConfig(start = Start.Cron("* * * * *")))
        }
    }
}
