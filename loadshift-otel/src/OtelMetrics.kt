package loadshift.otel

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import loadshift.core.Metrics
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.DurationUnit

class OtelMetrics(
    openTelemetry: OpenTelemetry,
    instrumentationName: String = "loadshift",
) : Metrics {

    private val meter = openTelemetry.getMeter(instrumentationName)
    private val counters = ConcurrentHashMap<String, LongCounter>()
    private val histograms = ConcurrentHashMap<String, DoubleHistogram>()

    override fun increment(name: String, attributes: Map<String, String>, amount: Long) {
        counters.computeIfAbsent(name) { meter.counterBuilder(it).build() }.add(amount, attributes(attributes))
    }

    override fun record(name: String, attributes: Map<String, String>, duration: Duration) {
        histograms.computeIfAbsent(name) { meter.histogramBuilder(it).setUnit("s").build() }
            .record(duration.toDouble(DurationUnit.SECONDS), attributes(attributes))
    }

    private fun attributes(values: Map<String, String>): Attributes =
        Attributes.builder().apply { values.forEach { (key, value) -> put(key, value) } }.build()
}
