package loadshift.camunda7

import kotlinx.serialization.json.JsonObject

interface Camunda7Api {
    suspend fun deploy(name: String, resources: List<Pair<String, ByteArray>>): DeploymentDto

    suspend fun startInstance(processDefinitionKey: String, variables: Map<String, CamundaValue>, businessKey: String?): StartInstanceResponse

    suspend fun processInstances(processDefinitionKey: String, firstResult: Int, maxResults: Int): List<ProcessInstanceDto>

    suspend fun variableInstances(variableName: String, processInstanceIds: List<String>): List<VariableInstanceDto>

    suspend fun fetchAndLock(request: FetchAndLockRequest): List<ExternalTaskDto>

    suspend fun complete(taskId: String, request: CompleteRequest)

    suspend fun failure(taskId: String, request: FailureRequest)

    suspend fun bpmnError(taskId: String, request: BpmnErrorRequest)

    suspend fun unlock(taskId: String)

    suspend fun extendLock(taskId: String, request: ExtendLockRequest)

    suspend fun processInstanceCount(processDefinitionKey: String): Long

    suspend fun historicProcessInstances(processInstanceIds: List<String>): List<HistoricProcessInstanceDto>

    suspend fun deleteProcessInstance(processInstanceId: String)

    suspend fun correlateMessage(request: MessageRequest): Int

    suspend fun tasks(processDefinitionKey: String, firstResult: Int, maxResults: Int): List<TaskDto>

    suspend fun task(taskId: String): TaskDto?

    suspend fun completeTask(taskId: String, variables: Map<String, CamundaValue>): Boolean

    suspend fun latestProcessDefinition(processDefinitionKey: String): ProcessDefinitionDto?

    suspend fun generateMigration(sourceProcessDefinitionId: String, targetProcessDefinitionId: String): JsonObject

    suspend fun executeMigration(plan: JsonObject, processInstanceIds: List<String>)

    suspend fun signal(name: String)

    fun close()
}
