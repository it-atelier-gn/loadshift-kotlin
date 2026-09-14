package loadshift.otel

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds

class OtelMetricsTest {

    @Test
    fun countersSumPerAttributeSetAndHistogramsRecordSeconds() {
        val reader = InMemoryMetricReader.create()
        val sdk = OpenTelemetrySdk.builder()
            .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(reader).build())
            .build()
        val metrics = OtelMetrics(sdk)

        metrics.increment("loadshift.items", mapOf("workflow" to "ship", "state" to "done"), 2)
        metrics.increment("loadshift.items", mapOf("workflow" to "ship", "state" to "done"))
        metrics.increment("loadshift.items", mapOf("workflow" to "ship", "state" to "skipped"))
        metrics.record("loadshift.task.duration", mapOf("topic" to "pack"), 1500.milliseconds)

        val collected = reader.collectAllMetrics().associateBy { it.name }
        val items = collected.getValue("loadshift.items").longSumData.points
            .associate { it.attributes.get(AttributeKey.stringKey("state")) to it.value }
        assertEquals<Map<String?, Long>>(mapOf("done" to 3L, "skipped" to 1L), items)

        val duration = collected.getValue("loadshift.task.duration")
        assertEquals("s", duration.unit)
        val point = duration.histogramData.points.single()
        assertEquals(1, point.count)
        assertEquals(1.5, point.sum)
        assertEquals("pack", point.attributes.get(AttributeKey.stringKey("topic")))
    }
}
