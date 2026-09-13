package loadshift.camunda8

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

class Camunda8Backend(
    base: String = "http://localhost:8080",
    auth: Camunda8Auth = Camunda8Auth.None,
    private val client: Camunda8Client = Camunda8Client(base, auth),
) : EngineBackend {

    override val control = RunTracker("camunda8")

    override suspend fun <W : WorkItem> run(workflow: Workflow<W>, config: RunConfig): RunHandle =
        launch(workflow, config, attach = false)

    override suspend fun <W : WorkItem> attach(workflow: Workflow<W>, config: RunConfig): RunHandle =
        launch(workflow, config, attach = true)

    @OptIn(EngineApi::class)
    private suspend fun launch(workflow: Workflow<*>, config: RunConfig, attach: Boolean): RunHandle {
        val engineRun = EngineRun(workflow, config)
        val processes = BpmnCompiler.compile(workflow)
        for (process in processes) {
            Camunda8Dialect.decorate(process.model, process.serviceTasks) { ref -> engineRun.maxAttempts(ref.jobType) }
        }
        if (!config.dryRun) {
            client.deploy(
                processes.map { process ->
                    val out = ByteArrayOutputStream()
                    Bpmn.writeModelToStream(out, process.model)
                    "${process.key}.bpmn" to out.toByteArray()
                },
            )
        }
        val runner = EngineRunner(
            run = engineRun,
            driver = Camunda8Driver(client, "loadshift-${engineRun.runId}"),
            rootProcessId = workflow.root.key,
            processIds = processes.map { it.key },
            attach = attach,
        ).begin()
        control.track(workflow, runner, runner)
        return runner
    }
}
