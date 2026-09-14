package loadshift.camunda8

import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.Json
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
import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.bpm.model.bpmn.instance.FlowNode
import java.io.ByteArrayOutputStream

class Camunda8Backend(
    base: String = "http://localhost:8080",
    auth: Camunda8Auth = Camunda8Auth.None,
    private val tenantId: String? = null,
    registry: RunRegistry? = null,
    private val jobStream: Camunda8JobStream? = null,
    private val client: Camunda8Client = Camunda8Client(base, auth, tenantId),
) : EngineBackend {

    override val control = RunTracker("camunda8", registry = registry)

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
        client.broadcastSignal(name)
    }

    override suspend fun userTasks(workflow: Workflow<*>): List<UserTask> {
        val refs = workflow.humanTasks().associateBy { it.levelKey to EngineNames.userTask(it.step.id) }
        val tasks = buildList {
            for (processId in refs.keys.map { it.first }.distinct()) {
                var cursor: String? = null
                while (true) {
                    val page = client.openUserTasks(processId, cursor, PAGE_SIZE)
                    addAll(page.items)
                    cursor = page.page.endCursor
                    if (page.items.isEmpty() || cursor == null) break
                }
            }
        }
        val itemKeys = tasks.mapNotNull { it.processInstanceKey }.distinct()
            .chunked(PAGE_SIZE)
            .flatMap { client.variables(EngineNames.ITEM_KEY, it) }
            .associate { variable ->
                variable.processInstanceKey to variable.value
                    ?.let { (Json.parseToJsonElement(it) as? JsonPrimitive)?.contentOrNull }
                    ?.takeIf { it.isNotEmpty() }
            }
        return tasks.mapNotNull { task ->
            val ref = refs[task.processDefinitionId to task.elementId] ?: return@mapNotNull null
            UserTask(
                task.userTaskKey,
                ref.owner.key,
                ref.step.name,
                task.processInstanceKey?.let { itemKeys[it] },
                task.assignee,
                task.candidateGroups.ifEmpty { ref.step.candidateGroups },
            )
        }
    }

    override suspend fun completeUserTask(taskId: String, form: JsonObject): Boolean {
        val task = client.userTask(taskId) ?: return false
        if (task.state != null && task.state != "CREATED") return false
        val stepId = task.elementId?.let(EngineNames::userTaskStep) ?: return false
        return client.completeUserTask(taskId, JsonObject(mapOf(EngineNames.formVariable(stepId) to form)))
    }

    @OptIn(EngineApi::class)
    override suspend fun migrate(workflow: Workflow<*>): MigrationResult {
        val engineRun = EngineRun(workflow, RunConfig())
        val processes = BpmnCompiler.compile(workflow)
        val targets = deploy(processes, engineRun)
        var migrated = 0
        val failures = mutableListOf<MigrationFailure>()
        for (process in processes) {
            val target = targets[process.key] ?: continue
            val targetElements = flowNodeIds(process.model)
            val outdated = instances(process.key).filter { it.processDefinitionKey != null && it.processDefinitionKey != target }
            for ((source, group) in outdated.groupBy { it.processDefinitionKey.orEmpty() }) {
                val shared = try {
                    flowNodeIds(Bpmn.readModelFromStream(client.processDefinitionXml(source).byteInputStream())) intersect targetElements
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    failures += MigrationFailure(process.key, group.map { it.processInstanceKey }, e.message ?: e.toString())
                    continue
                }
                for (instance in group) {
                    try {
                        client.migrateInstance(instance.processInstanceKey, target, shared)
                        migrated++
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        failures += MigrationFailure(process.key, listOf(instance.processInstanceKey), e.message ?: e.toString())
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
        if (config.dryRun) decorate(deployed, engineRun) else deploy(deployed, engineRun)
        val runner = EngineRunner(
            run = engineRun,
            driver = Camunda8Driver(client, "loadshift-${engineRun.runId}", jobStream, tenantId),
            rootProcessId = workflow.root.key,
            processIds = processes.map { it.key },
            attach = attach,
            requeue = requeue,
            scheduleProcessId = schedule?.key,
        ).begin()
        control.track(workflow, runner, runner)
        return runner
    }

    @OptIn(EngineApi::class)
    private fun decorate(processes: List<CompiledProcess>, engineRun: EngineRun) {
        for (process in processes) {
            Camunda8Dialect.decorate(process.model, process.serviceTasks, process.versionTag) { ref ->
                engineRun.jobTypes.takeIf { ref.jobType in it }?.let { engineRun.maxAttempts(ref.jobType) }
            }
        }
    }

    @OptIn(EngineApi::class)
    private suspend fun deploy(processes: List<CompiledProcess>, engineRun: EngineRun): Map<String, String> {
        decorate(processes, engineRun)
        return client.deploy(
            processes.map { process ->
                val out = ByteArrayOutputStream()
                Bpmn.writeModelToStream(out, process.model)
                "${process.key}.bpmn" to out.toByteArray()
            },
        )
    }

    private fun flowNodeIds(model: BpmnModelInstance): Set<String> =
        model.getModelElementsByType(FlowNode::class.java).mapNotNull { it.id }.toSet()

    private suspend fun instances(processDefinitionId: String) = buildList {
        var cursor: String? = null
        while (true) {
            val page = client.activeInstances(processDefinitionId, cursor, PAGE_SIZE)
            addAll(page.items)
            cursor = page.page.endCursor
            if (page.items.isEmpty() || cursor == null) break
        }
    }

    private companion object {
        const val PAGE_SIZE = 100
    }
}
