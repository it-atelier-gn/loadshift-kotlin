# loadshift-kotlin

[![Maven Central](https://img.shields.io/maven-central/v/io.github.it-atelier-gn/loadshift-core)](https://central.sonatype.com/namespace/io.github.it-atelier-gn)
[![CI](https://github.com/it-atelier-gn/loadshift-kotlin/actions/workflows/ci.yml/badge.svg)](https://github.com/it-atelier-gn/loadshift-kotlin/actions)
[![Kotlin](https://img.shields.io/badge/kotlin-2.x-blueviolet?logo=kotlin)](https://kotlinlang.org/)
[![Kotlin Toolchain](https://img.shields.io/badge/build-kotlin--toolchain-blue)](https://github.com/JetBrains/kotlin-toolchain)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Write Camunda workflows in Kotlin instead of BPMN. The same code runs on Camunda 7, CIB seven and Camunda 8, or in-process for tests. Loadshift compiles each workflow to the BPMN of its engine and works the external tasks or jobs with your Kotlin task bodies, with retries, fan-out, messages, sagas and dead letters.

Moving from Camunda 7 to Camunda 8: **[one workflow, both engines](https://it-atelier-gn.github.io/loadshift-kotlin/camunda-7-to-8.html)**

Full examples and reference: **[it-atelier-gn.github.io/loadshift-kotlin](https://it-atelier-gn.github.io/loadshift-kotlin/)**

---

## Modules

| Module | Contents |
| --- | --- |
| `loadshift-core` | The DSL, run configuration, the `Backend` API and the BPMN compiler |
| `loadshift-local` | `LocalBackend`, which runs workflows in-process |
| `loadshift-camunda-7` | `Camunda7Backend` for Camunda 7 and CIB seven, working external tasks over the REST API |
| `loadshift-camunda-8` | `Camunda8Backend` for Camunda 8, working jobs over the REST API |
| `loadshift-camunda-8-stream` | `CamundaJobStream`, which lets `Camunda8Backend` receive jobs the engine pushes over gRPC, see [Job streaming](docs/engines.md#job-streaming-on-camunda-8) |
| `loadshift-web` | `ControlServer`, a web console and JSON API for the runs of a backend |
| `loadshift-sqlite` | `SqliteLogSink` for `log()` entries, `SqliteCheckpointStore` for checkpoints, `SqliteDeadLetterStore` for dead letters and `SqliteRunRegistry` for runs of several workers |
| `loadshift-otel` | `OtelTracer`, which emits an OpenTelemetry span per task, and `OtelMetrics`, which reports [metrics](docs/observability.md#metrics) through OpenTelemetry |
| `loadshift-micrometer` | `MicrometerMetrics`, which reports [metrics](docs/observability.md#metrics) to a Micrometer `MeterRegistry` |
| `loadshift-demo` | A runnable example workflow |
| `loadshift-bench` | A load test that measures throughput and item latency of a backend, see [Load tests](docs/development.md#load-tests) |

## Supported engines

| Engine | Backend | Image used by the end-to-end tests |
| --- | --- | --- |
| Camunda 7 | `Camunda7Backend` | `camunda/camunda-bpm-platform:run-7.24.0` |
| CIB seven | `Camunda7Backend` | `cibseven/cibseven:run-2.2.0` |
| Camunda 8 | `Camunda8Backend` | `camunda/camunda:8.9.19` |

| Engine | Requirement |
| --- | --- |
| Camunda 7, CIB seven | History level `activity` or higher. A run detects finished process instances through the history API. |
| Camunda 8 | REST API v2 with secondary storage. A run detects finished process instances and message subscriptions through the search API. |

---

## Add as a dependency

Published to Maven Central under group `io.github.it-atelier-gn`. Replace `VERSION` with the version shown on the Maven Central badge at the top. Pick the modules you need.

<details>
<summary>Gradle (Kotlin DSL)</summary>

```kotlin
val loadshiftVersion = "VERSION"

dependencies {
    implementation("io.github.it-atelier-gn:loadshift-core:$loadshiftVersion")
    implementation("io.github.it-atelier-gn:loadshift-local:$loadshiftVersion")
    implementation("io.github.it-atelier-gn:loadshift-camunda-7:$loadshiftVersion")
    implementation("io.github.it-atelier-gn:loadshift-camunda-8:$loadshiftVersion")
    implementation("io.github.it-atelier-gn:loadshift-camunda-8-stream:$loadshiftVersion")
    implementation("io.github.it-atelier-gn:loadshift-web:$loadshiftVersion")
    implementation("io.github.it-atelier-gn:loadshift-sqlite:$loadshiftVersion")
    implementation("io.github.it-atelier-gn:loadshift-otel:$loadshiftVersion")
    implementation("io.github.it-atelier-gn:loadshift-micrometer:$loadshiftVersion")
}
```
</details>

<details>
<summary>Gradle (Groovy DSL)</summary>

```groovy
def loadshiftVersion = 'VERSION'

dependencies {
    implementation "io.github.it-atelier-gn:loadshift-core:$loadshiftVersion"
    implementation "io.github.it-atelier-gn:loadshift-local:$loadshiftVersion"
    implementation "io.github.it-atelier-gn:loadshift-camunda-7:$loadshiftVersion"
    implementation "io.github.it-atelier-gn:loadshift-camunda-8:$loadshiftVersion"
    implementation "io.github.it-atelier-gn:loadshift-camunda-8-stream:$loadshiftVersion"
    implementation "io.github.it-atelier-gn:loadshift-web:$loadshiftVersion"
    implementation "io.github.it-atelier-gn:loadshift-sqlite:$loadshiftVersion"
    implementation "io.github.it-atelier-gn:loadshift-otel:$loadshiftVersion"
    implementation "io.github.it-atelier-gn:loadshift-micrometer:$loadshiftVersion"
}
```
</details>

<details>
<summary>Maven</summary>

```xml
<properties>
    <loadshift.version>VERSION</loadshift.version>
</properties>

<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-core</artifactId>
    <version>${loadshift.version}</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-local</artifactId>
    <version>${loadshift.version}</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-camunda-7</artifactId>
    <version>${loadshift.version}</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-camunda-8</artifactId>
    <version>${loadshift.version}</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-camunda-8-stream</artifactId>
    <version>${loadshift.version}</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-web</artifactId>
    <version>${loadshift.version}</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-sqlite</artifactId>
    <version>${loadshift.version}</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-otel</artifactId>
    <version>${loadshift.version}</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-micrometer</artifactId>
    <version>${loadshift.version}</version>
</dependency>
```
</details>

---

## Requirements

- JDK 25 or newer. The libraries are compiled for Java 25.
- Docker, for the end-to-end tests and the local dev engines.
- `loadshift-sqlite` loads the native SQLite library through `sqlite-jdbc`. Start the JVM with `--enable-native-access=ALL-UNNAMED` so that JDK 25 does not print a restricted-method warning.

## Build and run

| Command | Effect |
| --- | --- |
| `./kotlin build` | Compiles all modules |
| `./kotlin test` | Runs all tests; the end-to-end suites run when Docker is available |
| `./kotlin run -m loadshift-demo` | Runs the demo workflow in-process |
| `./kotlin run -m loadshift-demo -- --ui` | Runs the demo with the web console at http://127.0.0.1:8571 |

On Windows, the wrapper is `.\kotlin`.

---

## A workflow

```kotlin
@Serializable
data class Order(val id: String, var total: Double = 0.0) : WorkItem {
    override val key get() = id
}

@Serializable
data class Line(val sku: String, val amount: Double) : WorkItem {
    override val key get() = sku
}

val fulfilment = workflow<Order>("fulfilment") {
    input(openOrders)

    task("reserve", retry = RetryPolicy(maxAttempts = 5)) { reserve(it) } compensate { release(it) }

    fanOut(expand = { order -> linesOf(order.id) }, concurrency = 8) {
        task("price") { line -> price(line) }
    }.reduce(0.0, combine = { sum, line -> sum + line.amount }) { order, total ->
        order.total = total
    }

    awaitMessage("payment-confirmed")
    task("ship", timeout = 30.seconds) { ship(it) }
}

val handle = Camunda8Backend("http://localhost:8080").run(fulfilment)
handle.send("payment-confirmed", "order-17")
val result = handle.await()
```

## Documentation

| Topic | Contents |
| --- | --- |
| [Workflows](docs/workflows.md) | Steps, signals, user tasks, called workflows, workflow keys |
| [Run configuration](docs/configuration.md) | `RunConfig` options, starts and cron expressions, error handling, dead-letter store, run handle |
| [Running on an engine](docs/engines.md) | Delivery and durability, handover, schedules, job streaming, versions and migration, authentication, tenants, process variables |
| [Web console](docs/web-console.md) | `ControlServer`, OpenID Connect, JSON API, runs of several workers |
| [Logging, tracing and metrics](docs/observability.md) | Log entries, OpenTelemetry spans, metrics |
| [Development](docs/development.md) | End-to-end tests, load tests, local dev engines, BPMN examples, dependency check, publishing, docs site |

## Contributing

Contributions are welcome. For substantial changes, open an issue first to discuss the approach.

## License

MIT, see [LICENSE](LICENSE).
