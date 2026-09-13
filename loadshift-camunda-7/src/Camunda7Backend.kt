package loadshift.camunda7

import loadshift.core.BpmnCompiler
import loadshift.core.ControllableBackend
import loadshift.core.EngineApi
import loadshift.core.EngineRun
import loadshift.core.EngineRunner
import loadshift.core.RunConfig
import loadshift.core.RunHandle
import loadshift.core.RunTracker
import loadshift.core.WorkItem
import loadshift.core.Workflow
import org.camunda.bpm.model.bpmn.Bpmn
import java.io.ByteArrayOutputStream

class Camunda7Backend(
    base: String = "http://localhost:8080/engine-rest",
    private val client: Camunda7Client = Camunda7Client(base),
) : ControllableBackend {

    override val control = RunTracker("camunda7")

    @OptIn(EngineApi::class)
    override suspend fun <W : WorkItem> run(workflow: Workflow<W>, config: RunConfig): RunHandle {
        val engineRun = EngineRun(workflow, config)
        val processes = BpmnCompiler.compile(workflow)
        for (process in processes) Camunda7Dialect.decorate(process.model, process.serviceTasks)
        if (!config.dryRun) {
            client.deploy(
                workflow.name,
                processes.map { process ->
                    val out = ByteArrayOutputStream()
                    Bpmn.writeModelToStream(out, process.model)
                    "${process.key}.bpmn" to out.toByteArray()
                },
            )
        }
        val runner = EngineRunner(
            run = engineRun,
            driver = Camunda7Driver(client, "loadshift-${engineRun.runId}"),
            rootProcessId = workflow.root.key,
            processIds = processes.map { it.key },
        ).begin()
        control.track(workflow, runner, runner)
        return runner
    }
}
