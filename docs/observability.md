# Logging, tracing and metrics

## Logging and tracing

```kotlin
task("charge") { order ->
    log("charging customer", "orderId" to order.id, "amount" to order.total)
}

backend.run(workflow, RunConfig(logSink = SqliteLogSink("logs.db"), tracer = OtelTracer(openTelemetry)))
```

Each log entry carries the run id, workflow name, the keys of the parent items, the item key and the task topic.

`SqliteLogSink` and `InMemoryLogSink` also implement `LogReader`: `list(runId, limit, after)` returns the entries of one run in writing order, with a cursor for the next page. `InMemoryLogSink` keeps the 10,000 most recent entries.

## Metrics

```kotlin
backend.run(workflow, RunConfig(metrics = MicrometerMetrics(meterRegistry)))
backend.run(workflow, RunConfig(metrics = OtelMetrics(openTelemetry)))
```

| Metric | Type | Attributes | Measures |
| --- | --- | --- | --- |
| `loadshift.items` | Counter | `workflow`, `state` | Changes of the run counters; `state` is `seeded`, `expanded`, `done`, `failed`, `skipped` or `cancelled` |
| `loadshift.task.duration` | Timer (OpenTelemetry: histogram in seconds) | `workflow`, `topic`, `outcome` | Each attempt of a task body; `outcome` is `success` or `failure` |
| `loadshift.task.retries` | Counter | `workflow`, `topic` | Failed attempts that are retried |
| `loadshift.dead.letters` | Counter | `workflow`, `topic` | Dead letters, including failed compensations |
| `loadshift.lock.extensions` | Counter | `workflow`, `topic`, `outcome` | Job lock extensions on an engine |

`workflow` is the workflow key. Implement `Metrics` to report to another system.
