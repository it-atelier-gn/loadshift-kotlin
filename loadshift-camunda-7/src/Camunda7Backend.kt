package loadshift.camunda7

import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import loadshift.core.BpmnCompiler
import loadshift.core.EngineNames
import loadshift.core.UserTask
import loadshift.core.humanTasks
import loadshift.core.CompiledProcess
import loadshift.core.DeadLetterRecord
import loadshift.core.EngineApi
import loadshift.core.EngineBackend
import loadshift.core.EngineRun
import loadshift.core.EngineRunner
import loadshift.core.MigrationFailure
import loadshift.core.MigrationResult
import loadshift.core.RunConfig
import loadshift.core.RunHandle
import loadshift.core.RunRegistry
import loadshift.core.RunTracker
import loadshift.core.Start
import loadshift.core.WorkItem
import loadshift.core.Workflow
import loadshift.core.requireRequeueable
import org.camunda.bpm.model.bpmn.Bpmn
import java.io.ByteArrayOutputStream

class Camunda7Backend(
    base: String = "http://localhost:8080/engine-rest",
    credentials: BasicCredentials? = null,
    tenantId: String? = null,
    registry: RunRegistry? = null,
    private val client: Camunda7Client = Camunda7Client(base, credentials, tenantId),
) : EngineBackend {

    override val control = RunTracker("camunda7", registry = registry)

    override suspend fun <W : WorkItem> run(workflow: Workflow<W>, config: RunConfig): RunHandle =
        launch(workflow, config, attach = false, requeue = null)

    override suspend fun <W : WorkItem> attach(workflow: Workflow<W>, config: RunConfig): RunHandle =
        launch(workflow, config, attach = true, requeue = null)

    override suspend fun <W : WorkItem> requeue(
        workflow: Workflow<W>,
        records: List<DeadLetterRecord>,
        config: RunConfig,
    ): RunHandle {
        workflow.requireRequeueable(records, config)
        return launch(workflow, config, attach = false, requeue = records)
    }

    override suspend fun signal(name: String) {
        client.signal(name)
    }

    override suspend fun userTasks(workflow: Workflow<*>): List<UserTask> {
        val refs = workflow.humanTasks().associateBy { it.levelKey to EngineNames.userTask(it.step.id) }
        val tasks = buildList {
            for (processKey in refs.keys.map { it.first }.distinct()) {
                var first = 0
                while (true) {
                    val page = client.tasks(processKey, first, PAGE_SIZE)
                    page.mapTo(this) { processKey to it }
                    if (page.size < PAGE_SIZE) break
                    first += PAGE_SIZE
                }
            }
        }
        val itemKeys = tasks.mapNotNull { it.second.processInstanceId }.distinct()
            .chunked(QUERY_CHUNK)
            .flatMap { client.variableInstances(EngineNames.ITEM_KEY, it) }
            .mapNotNull { variable ->
                val instance = variable.processInstanceId ?: return@mapNotNull null
                instance to (variable.value as? JsonPrimitive)?.contentOrNull?.takeIf { it.isNotEmpty() }
            }
            .toMap()
        return tasks.mapNotNull { (processKey, task) ->
            val ref = refs[processKey to task.taskDefinitionKey] ?: return@mapNotNull null
            UserTask(task.id, ref.owner.key, ref.step.name, itemKeys[task.processInstanceId], task.assignee, ref.step.candidateGroups)
        }
    }

    override suspend fun completeUserTask(taskId: String, form: JsonObject): Boolean {
        val task = client.task(taskId) ?: return false
        val stepId = task.taskDefinitionKey?.let(EngineNames::userTaskStep) ?: return false
        val variables = CamundaVariables.toCamunda(JsonObject(mapOf(EngineNames.formVariable(stepId) to form)))
        return client.completeTask(taskId, variables)
    }

    override suspend fun migrate(workflow: Workflow<*>): MigrationResult {
        val processes = BpmnCompiler.compile(workflow)
        deploy(workflow, processes)
        var migrated = 0
        val failures = mutableListOf<MigrationFailure>()
        for (process in processes) {
            val target = client.latestProcessDefinition(process.key) ?: continue
            val outdated = instances(process.key).filter { it.definitionId != null && it.definitionId != target.id }
            for ((source, group) in outdated.groupBy { it.definitionId.orEmpty() }) {
                val plan = try {
                    client.generateMigration(source, target.id)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    failures += MigrationFailure(process.key, group.map { it.id }, e.message ?: e.toString())
                    continue
                }
                for (chunk in group.map { it.id }.chunked(MIGRATION_CHUNK)) {
                    try {
                        client.executeMigration(plan, chunk)
                        migrated += chunk.size
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        failures += MigrationFailure(process.key, chunk, e.message ?: e.toString())
                    }
                }
            }
        }
        return MigrationResult(migrated, failures)
    }

    @OptIn(EngineApi::class)
    private suspend fun launch(
        workflow: Workflow<*>,
        config: RunConfig,
        attach: Boolean,
        requeue: List<DeadLetterRecord>?,
    ): RunHandle {
        val engineRun = EngineRun(workflow, config)
        val processes = BpmnCompiler.compile(workflow)
        val schedule = if (config.start is Start.Cron && !attach) BpmnCompiler.compileSchedule(workflow) else null
        val deployed = processes + listOfNotNull(schedule)
        if (config.dryRun) {
            for (process in deployed) Camunda7Dialect.decorate(process.model, process.serviceTasks, process.versionTag)
        } else {
            deploy(workflow, deployed)
        }
        val runner = EngineRunner(
            run = engineRun,
            driver = Camunda7Driver(client, "loadshift-${engineRun.runId}"),
            rootProcessId = workflow.root.key,
            processIds = processes.map { it.key },
            attach = attach,
            requeue = requeue,
            scheduleProcessId = schedule?.key,
        ).begin()
        control.track(workflow, runner, runner)
        return runner
    }

    private suspend fun deploy(workflow: Workflow<*>, processes: List<CompiledProcess>) {
        for (process in processes) Camunda7Dialect.decorate(process.model, process.serviceTasks, process.versionTag)
        client.deploy(
            workflow.name,
            processes.map { process ->
                val out = ByteArrayOutputStream()
                Bpmn.writeModelToStream(out, process.model)
                "${process.key}.bpmn" to out.toByteArray()
            },
        )
    }

    private suspend fun instances(processDefinitionKey: String): List<ProcessInstanceDto> = buildList {
        var first = 0
        while (true) {
            val page = client.processInstances(processDefinitionKey, first, PAGE_SIZE)
            addAll(page)
            if (page.size < PAGE_SIZE) break
            first += PAGE_SIZE
        }
    }

    private companion object {
        const val PAGE_SIZE = 500
        const val MIGRATION_CHUNK = 500
        const val QUERY_CHUNK = 100
    }
}
