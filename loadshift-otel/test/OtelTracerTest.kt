package loadshift.otel

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OtelTracerTest {

    private fun tracerWith(exporter: InMemorySpanExporter): OtelTracer {
        val sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(
                SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                    .build(),
            )
            .build()
        return OtelTracer(sdk)
    }

    @Test
    fun recordsSpanWithAttributesAndReturnsBody() = runTest {
        val exporter = InMemorySpanExporter.create()
        val tracer = tracerWith(exporter)
        val result = tracer.span("task charge", mapOf("item" to "a")) { 42 }
        assertEquals(42, result)
        val spans = exporter.finishedSpanItems
        assertEquals(1, spans.size)
        assertEquals("task charge", spans[0].name)
        assertEquals("a", spans[0].attributes.get(AttributeKey.stringKey("item")))
    }

    @Test
    fun marksSpanErrorOnException() = runTest {
        val exporter = InMemorySpanExporter.create()
        val tracer = tracerWith(exporter)
        runCatching { tracer.span("task boom") { throw RuntimeException("nope") } }
        val span = exporter.finishedSpanItems.single()
        assertTrue(span.status.statusCode.name == "ERROR")
    }
}
