package loadshift.camunda7

import loadshift.core.BpmnCompiler
import loadshift.core.EngineApi
import loadshift.core.EngineBackend
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
    credentials: BasicCredentials? = null,
    private val client: Camunda7Client = Camunda7Client(base, credentials),
) : EngineBackend {

    override val control = RunTracker("camunda7")

    override suspend fun <W : WorkItem> run(workflow: Workflow<W>, config: RunConfig): RunHandle =
        launch(workflow, config, attach = false)

    override suspend fun <W : WorkItem> attach(workflow: Workflow<W>, config: RunConfig): RunHandle =
        launch(workflow, config, attach = true)

    @OptIn(EngineApi::class)
    private suspend fun launch(workflow: Workflow<*>, config: RunConfig, attach: Boolean): RunHandle {
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
            attach = attach,
        ).begin()
        control.track(workflow, runner, runner)
        return runner
    }
}
