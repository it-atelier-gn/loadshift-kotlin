package loadshift.core

data class MigrationFailure(val processId: String, val instanceIds: List<String>, val error: String)

data class MigrationResult(val migrated: Int, val failures: List<MigrationFailure>)
