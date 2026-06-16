package loadshift.core

class Workflow<W : WorkItem>(
    val key: String,
    val name: String,
    val seed: Seed<W>,
    val root: SubFlow<W>,
)

interface Backend {
    suspend fun <W : WorkItem> run(workflow: Workflow<W>, config: RunConfig = RunConfig()): RunHandle
}
