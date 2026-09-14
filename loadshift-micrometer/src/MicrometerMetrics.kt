package loadshift.micrometer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import loadshift.core.Metrics
import kotlin.time.Duration
import kotlin.time.toJavaDuration

class MicrometerMetrics(private val registry: MeterRegistry) : Metrics {

    override fun increment(name: String, attributes: Map<String, String>, amount: Long) {
        registry.counter(name, tags(attributes)).increment(amount.toDouble())
    }

    override fun record(name: String, attributes: Map<String, String>, duration: Duration) {
        registry.timer(name, tags(attributes)).record(duration.toJavaDuration())
    }

    private fun tags(attributes: Map<String, String>): Tags = Tags.of(attributes.map { (key, value) -> Tag.of(key, value) })
}
