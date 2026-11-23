package loadshift.camunda7.embedded

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import loadshift.camunda7.BpmnErrorRequest
import loadshift.camunda7.Camunda7Api
import loadshift.camunda7.Camunda7Backend
import loadshift.camunda7.CamundaValue
import loadshift.camunda7.CamundaVariables
import loadshift.camunda7.CompleteRequest
import loadshift.camunda7.DeploymentDto
import loadshift.camunda7.ExtendLockRequest
import loadshift.camunda7.ExternalTaskDto
import loadshift.camunda7.FailureRequest
import loadshift.camunda7.FetchAndLockRequest
import loadshift.camunda7.HistoricProcessInstanceDto
import loadshift.camunda7.MessageRequest
import loadshift.camunda7.ProcessDefinitionDto
import loadshift.camunda7.ProcessInstanceDto
import loadshift.camunda7.StartInstanceResponse
import loadshift.camunda7.TaskDto
import loadshift.camunda7.VariableInstanceDto
import loadshift.core.RunRegistry
import org.cibseven.bpm.engine.ProcessEngine
import org.cibseven.bpm.engine.ProcessEngineException
import org.cibseven.bpm.engine.exception.NotFoundException
import org.cibseven.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl
import org.cibseven.bpm.engine.impl.cfg.StandaloneProcessEngineConfiguration
import org.cibseven.bpm.engine.impl.jobexecutor.DefaultJobExecutor
import org.cibseven.bpm.engine.variable.VariableMap
import org.cibseven.bpm.engine.variable.Variables
import org.cibseven.bpm.engine.variable.value.SerializableValue
import org.cibseven.bpm.engine.variable.value.TypedValue
import org.cibseven.spin.plugin.impl.SpinProcessEnginePlugin
import org.cibseven.spin.plugin.variable.SpinValues
import java.io.ByteArrayInputStream
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

class EmbeddedDatabase(
    val jdbcUrl: String,
    val username: String = "sa",
    val password: String = "",
    val driver: String = "org.h2.Driver",
) {
    override fun toString(): String = "EmbeddedDatabase(jdbcUrl=$jdbcUrl, username=$username)"

    companion object {
        fun inMemory(name: String = "loadshift-${UUID.randomUUID()}"): EmbeddedDatabase =
            EmbeddedDatabase("jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1")
    }
}

class EmbeddedCamunda7 private constructor(
    val processEngine: ProcessEngine,
    private val tenantId: String?,
    private val ownsEngine: Boolean,
) : Camunda7Api, AutoCloseable {

    constructor(processEngine: ProcessEngine, tenantId: String? = null) : this(processEngine, tenantId, false)

    constructor(
        database: EmbeddedDatabase = EmbeddedDatabase.inMemory(),
        tenantId: String? = null,
        jobExecutor: Boolean = true,
        configure: (ProcessEngineConfigurationImpl) -> Unit = {},
    ) : this(buildEngine(database, jobExecutor, configure), tenantId, true)

    private val runtime = processEngine.runtimeService
    private val repository = processEngine.repositoryService
    private val externalTasks = processEngine.externalTaskService
    private val history = processEngine.historyService
    private val taskService = processEngine.taskService

    fun backend(registry: RunRegistry? = null): Camunda7Backend = Camunda7Backend(registry = registry, client = this)

    override suspend fun deploy(name: String, resources: List<Pair<String, ByteArray>>): DeploymentDto = io {
        val builder = repository.createDeployment().name(name).enableDuplicateFiltering(true)
        tenantId?.let { builder.tenantId(it) }
        for ((fileName, bytes) in resources) builder.addInputStream(fileName, ByteArrayInputStream(bytes))
        val deployment = builder.deploy()
        DeploymentDto(deployment.id, deployment.name, deployment.deploymentTime?.toInstant()?.toString())
    }

    override suspend fun startInstance(
        processDefinitionKey: String,
        variables: Map<String, CamundaValue>,
        businessKey: String?,
    ): StartInstanceResponse = io {
        val builder = runtime.createProcessInstanceByKey(processDefinitionKey).setVariables(toEngine(variables))
        if (tenantId != null) builder.processDefinitionTenantId(tenantId)
        if (businessKey != null) builder.businessKey(businessKey)
        val instance = builder.execute()
        StartInstanceResponse(instance.id, instance.processDefinitionId, instance.businessKey)
    }

    override suspend fun processInstances(processDefinitionKey: String, firstResult: Int, maxResults: Int): List<ProcessInstanceDto> = io {
        val query = runtime.createProcessInstanceQuery().processDefinitionKey(processDefinitionKey)
        tenantId?.let { query.tenantIdIn(it) }
        query.orderByProcessInstanceId().asc().listPage(firstResult, maxResults)
            .map { ProcessInstanceDto(it.id, it.businessKey, it.processDefinitionId) }
    }

    override suspend fun variableInstances(variableName: String, processInstanceIds: List<String>): List<VariableInstanceDto> {
        if (processInstanceIds.isEmpty()) return emptyList()
        return io {
            runtime.createVariableInstanceQuery()
                .variableName(variableName)
                .processInstanceIdIn(*processInstanceIds.toTypedArray())
                .disableCustomObjectDeserialization()
                .list()
                .map { variable ->
                    val value = fromEngine(variable.typedValue)
                    VariableInstanceDto(variable.processInstanceId, value.value, value.type)
                }
        }
    }

    override suspend fun fetchAndLock(request: FetchAndLockRequest): List<ExternalTaskDto> {
        val deadline = TimeSource.Monotonic.markNow() + (request.asyncResponseTimeout ?: 0L).milliseconds
        while (true) {
            val tasks = io { lock(request) }
            if (tasks.isNotEmpty() || deadline.hasPassedNow()) return tasks
            delay(minOf(FETCH_POLL, -deadline.elapsedNow()))
        }
    }

    private fun lock(request: FetchAndLockRequest): List<ExternalTaskDto> {
        val builder = externalTasks.fetchAndLock(request.maxTasks, request.workerId, request.usePriority)
        for (topic in request.topics) {
            val topicBuilder = builder.topic(topic.topicName, topic.lockDuration)
            topic.variables?.let { topicBuilder.variables(it) }
            (topic.tenantIdIn ?: tenantId?.let(::listOf))?.let { topicBuilder.tenantIdIn(*it.toTypedArray()) }
        }
        return builder.execute().map { task ->
            ExternalTaskDto(
                id = task.id,
                topicName = task.topicName,
                processInstanceId = task.processInstanceId,
                processDefinitionKey = task.processDefinitionKey,
                retries = task.retries,
                variables = fromEngine(task.variables),
            )
        }
    }

    override suspend fun complete(taskId: String, request: CompleteRequest) = io {
        externalTasks.complete(taskId, request.workerId, toEngine(request.variables))
    }

    override suspend fun failure(taskId: String, request: FailureRequest) = io {
        externalTasks.handleFailure(taskId, request.workerId, request.errorMessage, request.errorDetails, request.retries, request.retryTimeout)
    }

    override suspend fun bpmnError(taskId: String, request: BpmnErrorRequest) = io {
        externalTasks.handleBpmnError(taskId, request.workerId, request.errorCode, request.errorMessage, toEngine(request.variables.orEmpty()))
    }

    override suspend fun unlock(taskId: String) = io {
        try {
            externalTasks.unlock(taskId)
        } catch (_: NotFoundException) {
        }
    }

    override suspend fun extendLock(taskId: String, request: ExtendLockRequest) = io {
        externalTasks.extendLock(taskId, request.workerId, request.newDuration)
    }

    override suspend fun processInstanceCount(processDefinitionKey: String): Long = io {
        val query = runtime.createProcessInstanceQuery().processDefinitionKey(processDefinitionKey)
        tenantId?.let { query.tenantIdIn(it) }
        query.count()
    }

    override suspend fun historicProcessInstances(processInstanceIds: List<String>): List<HistoricProcessInstanceDto> {
        if (processInstanceIds.isEmpty()) return emptyList()
        return io {
            history.createHistoricProcessInstanceQuery()
                .processInstanceIds(processInstanceIds.toSet())
                .finished()
                .list()
                .map { HistoricProcessInstanceDto(it.id, it.state) }
        }
    }

    override suspend fun deleteProcessInstance(processInstanceId: String) = io {
        runtime.deleteProcessInstanceIfExists(processInstanceId, null, true, true, false, false)
    }

    override suspend fun correlateMessage(request: MessageRequest): Int = io {
        val correlation = runtime.createMessageCorrelation(request.messageName)
        for ((name, value) in request.correlationKeys.orEmpty()) {
            correlation.processInstanceVariableEquals(name, CamundaVariables.decode(value))
        }
        (request.tenantId ?: tenantId)?.let { correlation.tenantId(it) }
        request.processVariables?.let { correlation.setVariables(toEngine(it)) }
        try {
            if (request.all) {
                correlation.correlateAllWithResult().size
            } else {
                correlation.correlateWithResult()
                1
            }
        } catch (_: ProcessEngineException) {
            0
        }
    }

    override suspend fun tasks(processDefinitionKey: String, firstResult: Int, maxResults: Int): List<TaskDto> = io {
        val query = taskService.createTaskQuery().processDefinitionKey(processDefinitionKey)
        tenantId?.let { query.tenantIdIn(it) }
        query.orderByTaskId().asc().listPage(firstResult, maxResults).map(::taskDto)
    }

    override suspend fun task(taskId: String): TaskDto? = io {
        taskService.createTaskQuery().taskId(taskId).singleResult()?.let(::taskDto)
    }

    override suspend fun completeTask(taskId: String, variables: Map<String, CamundaValue>): Boolean = io {
        if (taskService.createTaskQuery().taskId(taskId).count() == 0L) {
            false
        } else {
            taskService.complete(taskId, toEngine(variables))
            true
        }
    }

    override suspend fun latestProcessDefinition(processDefinitionKey: String): ProcessDefinitionDto? = io {
        val query = repository.createProcessDefinitionQuery().processDefinitionKey(processDefinitionKey).latestVersion()
        if (tenantId == null) query.withoutTenantId() else query.tenantIdIn(tenantId)
        query.singleResult()?.let { ProcessDefinitionDto(it.id, it.key, it.version, it.versionTag) }
    }

    override suspend fun generateMigration(sourceProcessDefinitionId: String, targetProcessDefinitionId: String): JsonObject = io {
        plan(sourceProcessDefinitionId, targetProcessDefinitionId)
        JsonObject(
            mapOf(
                SOURCE to JsonPrimitive(sourceProcessDefinitionId),
                TARGET to JsonPrimitive(targetProcessDefinitionId),
            ),
        )
    }

    override suspend fun executeMigration(plan: JsonObject, processInstanceIds: List<String>) = io {
        val migration = plan(plan.getValue(SOURCE).jsonPrimitive.content, plan.getValue(TARGET).jsonPrimitive.content)
        runtime.newMigration(migration).processInstanceIds(processInstanceIds).skipCustomListeners().execute()
    }

    override suspend fun signal(name: String) = io {
        val event = runtime.createSignalEvent(name)
        tenantId?.let { event.tenantId(it) }
        event.send()
    }

    override fun close() {
        if (ownsEngine) processEngine.close()
    }

    private fun plan(source: String, target: String) =
        runtime.createMigrationPlan(source, target).mapEqualActivities().build()

    private fun taskDto(task: org.cibseven.bpm.engine.task.Task) =
        TaskDto(task.id, task.name, task.assignee, task.taskDefinitionKey, task.processInstanceId, task.processDefinitionId)

    private suspend fun <T> io(block: () -> T): T = withContext(Dispatchers.IO) { block() }

    internal companion object {
        private const val SOURCE = "sourceProcessDefinitionId"
        private const val TARGET = "targetProcessDefinitionId"
        private val FETCH_POLL = 100.milliseconds

        private fun buildEngine(
            database: EmbeddedDatabase,
            jobExecutor: Boolean,
            configure: (ProcessEngineConfigurationImpl) -> Unit,
        ): ProcessEngine {
            val configuration = StandaloneProcessEngineConfiguration()
            configuration.processEngineName = "loadshift-${UUID.randomUUID()}"
            configuration.jdbcUrl = database.jdbcUrl
            configuration.jdbcUsername = database.username
            configuration.jdbcPassword = database.password
            configuration.jdbcDriver = database.driver
            configuration.databaseSchemaUpdate = ProcessEngineConfigurationImpl.DB_SCHEMA_UPDATE_TRUE
            configuration.isJobExecutorActivate = jobExecutor
            configuration.jobExecutor = DefaultJobExecutor().apply {
                waitTimeInMillis = 200
                maxWait = 1000
            }
            configuration.processEnginePlugins = mutableListOf(SpinProcessEnginePlugin())
            configure(configuration)
            return configuration.buildProcessEngine()
        }

        internal fun toEngine(variables: Map<String, CamundaValue>): VariableMap {
            val map = Variables.createVariables()
            for ((name, value) in variables) map.putValueTyped(name, toTyped(value))
            return map
        }

        internal fun toTyped(value: CamundaValue): TypedValue =
            if (value.type.equals("json", ignoreCase = true) && value.value is JsonPrimitive) {
                SpinValues.jsonValue(value.value.jsonPrimitive.content).create()
            } else {
                Variables.untypedValue(CamundaVariables.decode(value))
            }

        internal fun fromEngine(variables: VariableMap): Map<String, CamundaValue> =
            variables.keys.associateWith { fromEngine(variables.getValueTyped<TypedValue>(it)) }

        internal fun fromEngine(value: TypedValue?): CamundaValue {
            if (value == null) return CamundaValue(JsonNull, "Null")
            if (value is SerializableValue) {
                val serialized = value.valueSerialized ?: return CamundaValue(JsonNull, "Null")
                val type = if (value.type?.name.equals("json", ignoreCase = true)) "Json" else "String"
                return CamundaValue(JsonPrimitive(serialized), type)
            }
            return CamundaVariables.encode(value.value)
        }
    }
}
