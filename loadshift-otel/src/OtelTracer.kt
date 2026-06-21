package loadshift.otel

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.StatusCode
import loadshift.core.Tracer

class OtelTracer(
    openTelemetry: OpenTelemetry,
    instrumentationName: String = "loadshift",
) : Tracer {

    private val tracer = openTelemetry.getTracer(instrumentationName)

    override suspend fun <T> span(name: String, attributes: Map<String, String>, body: suspend () -> T): T {
        val span = tracer.spanBuilder(name).startSpan()
        attributes.forEach { (k, v) -> span.setAttribute(k, v) }
        return try {
            body()
        } catch (e: Throwable) {
            span.setStatus(StatusCode.ERROR, e.message ?: e::class.simpleName ?: "error")
            span.recordException(e)
            throw e
        } finally {
            span.end()
        }
    }
}
