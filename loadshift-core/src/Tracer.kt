package loadshift.core

interface Tracer {
    suspend fun <T> span(name: String, attributes: Map<String, String> = emptyMap(), body: suspend () -> T): T
}

object NoopTracer : Tracer {
    override suspend fun <T> span(name: String, attributes: Map<String, String>, body: suspend () -> T): T = body()
}
