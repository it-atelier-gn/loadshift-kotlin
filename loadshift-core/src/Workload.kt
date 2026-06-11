package loadshift.core

class Workflow<W : WorkItemBase>(
    val key: String,
    val name: String,
    val seed: Seed<W>,
    val root: SubFlow<W>,
)

interface Backend {
    suspend fun <W : WorkItemBase> run(workflow: Workflow<W>, config: RunConfig = RunConfig()): RunHandle
}
