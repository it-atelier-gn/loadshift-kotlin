# loadshift-kotlin

[![CI](https://github.com/it-atelier-gn/loadshift-kotlin/actions/workflows/ci.yml/badge.svg)](https://github.com/it-atelier-gn/loadshift-kotlin/actions)
[![Kotlin](https://img.shields.io/badge/kotlin-2.x-blueviolet?logo=kotlin)](https://kotlinlang.org/)
[![Kotlin Toolchain](https://img.shields.io/badge/build-kotlin--toolchain-blue)](https://github.com/JetBrains/kotlin-toolchain)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Kotlin DSL for durable execution with Camunda.

Full examples and reference: **[it-atelier-gn.github.io/loadshift-kotlin](https://it-atelier-gn.github.io/loadshift-kotlin/)**

---

## Modules

| Module | Description |
| --- | --- |
| `loadshift-core` | Flow IR, the DSL, the `Backend` SPI, and the BPMN compiler (with a deterministic diagram layout). |
| `loadshift-local` | In-process interpreter (`LocalBackend`) for tests and development. |
| `loadshift-camunda-7` | `Camunda7Backend`. Compiles to BPMN and external tasks, driven over REST. |
| `loadshift-camunda-8` | `Camunda8Backend`. Camunda 8 REST API with `zeebe:` BPMN extensions. |
| `loadshift-web` | `ControlServer`. Embedded web console showing live runs of any backend. |
| `loadshift-sqlite` | `SqliteLogSink` and `SqliteCheckpointStore`. Persists `log()` entries and resumable-run checkpoints to SQLite. |
| `loadshift-otel` | `OtelTracer`. Emits OpenTelemetry spans per task to any OTel backend. |
| `loadshift-demo` | Runnable example on `LocalBackend`. |

---

## Add as a dependency

Published to Maven Central under group `io.github.it-atelier-gn`, version `0.5.0`. Pick the modules you need (see [Modules](#modules)).

<details>
<summary>Gradle (Kotlin DSL)</summary>

```kotlin
dependencies {
    implementation("io.github.it-atelier-gn:loadshift-core:0.5.0")
    implementation("io.github.it-atelier-gn:loadshift-local:0.5.0")
    implementation("io.github.it-atelier-gn:loadshift-camunda-7:0.5.0")
    implementation("io.github.it-atelier-gn:loadshift-camunda-8:0.5.0")
    implementation("io.github.it-atelier-gn:loadshift-web:0.5.0")
    implementation("io.github.it-atelier-gn:loadshift-sqlite:0.5.0")
}
```
</details>

<details>
<summary>Gradle (Groovy DSL)</summary>

```groovy
dependencies {
    implementation 'io.github.it-atelier-gn:loadshift-core:0.5.0'
    implementation 'io.github.it-atelier-gn:loadshift-local:0.5.0'
    implementation 'io.github.it-atelier-gn:loadshift-camunda-7:0.5.0'
    implementation 'io.github.it-atelier-gn:loadshift-camunda-8:0.5.0'
    implementation 'io.github.it-atelier-gn:loadshift-web:0.5.0'
    implementation 'io.github.it-atelier-gn:loadshift-sqlite:0.5.0'
}
```
</details>

<details>
<summary>Maven</summary>

```xml
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-core</artifactId>
    <version>0.5.0</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-local</artifactId>
    <version>0.5.0</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-camunda-7</artifactId>
    <version>0.5.0</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-camunda-8</artifactId>
    <version>0.5.0</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-web</artifactId>
    <version>0.5.0</version>
</dependency>
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-sqlite</artifactId>
    <version>0.5.0</version>
</dependency>
```
</details>

---

## Quick Start

### Prerequisites

- JDK 21+ (the bundled [Kotlin Toolchain](https://github.com/JetBrains/kotlin-toolchain) wrapper handles everything else)

### Build & Run

```sh
git clone https://github.com/it-atelier-gn/loadshift-kotlin.git
cd loadshift-kotlin
./kotlin build
./kotlin test

# run the demo workflow
./kotlin run -m loadshift-demo

# run the demo with the web console at http://127.0.0.1:8571
./kotlin run -m loadshift-demo -- --ui
```

---

## End-to-end tests

The Camunda modules contain full-loop e2e tests (`Camunda7E2eTest`, `Camunda8E2eTest`) that deploy a compiled workflow to a real engine, work the external tasks/jobs, and assert the run result. They use [Testcontainers](https://testcontainers.com/) to start minimal engines automatically:

- Camunda 7: `camunda/camunda-bpm-platform:run-7.24.0`
- Camunda 8: `camunda/camunda:8.9.8` with H2 secondary storage and the unprotected API (no Elasticsearch needed)

`./kotlin test` runs them whenever Docker is available; without Docker they skip. To reuse an already-running engine instead, set `LOADSHIFT_C7_BASE` (e.g. `http://localhost:8080/engine-rest`) or `LOADSHIFT_C8_BASE` (e.g. `http://localhost:8080`).

---

## Local dev engine

To develop and test workflows against a real engine, start an empty one with a single command (Docker and the Kotlin 2.3 CLI required):

```sh
.\kotlin run -m engine c7              # Camunda 7 Run on :8080 — engine-rest + Cockpit (demo/demo)
.\kotlin run -m engine cib7            # CIB seven (Camunda 7 fork) on :8080 — engine-rest + Cockpit (demo/demo)
.\kotlin run -m engine c8              # Camunda 8.9 on :8080 — REST v2 + Operate (demo/demo), H2 storage
.\kotlin run -m engine c8 logs         # follow logs
.\kotlin run -m engine c8 stop         # stop
.\kotlin run -m engine c7 start 9090   # custom port
```

---

## Examples: DSL → BPMN

`scripts/examples` compiles a set of example workflows with both Camunda dialects, verifies the BPMN structurally (service task topics, gateways, call activities, FEEL conditions, complete diagram interchange), and generates [docs/examples.html](https://it-atelier-gn.github.io/loadshift-kotlin/examples.html) — DSL source on the left, the rendered BPMN diagram (bpmn-js) on the right.

Requires the [Kotlin 2.3 command-line compiler](https://kotlinlang.org/docs/command-line.html):

```sh
.\kotlin run -m examples           # verify + regenerate docs/examples.html
.\kotlin run -m examples verify    # structural checks only
```

---

## Web Console

Every backend implements the control API (`ControllableBackend` in `loadshift-core`): run state, progress counters, dead letters, the flow structure, and start/pause/cancel. `loadshift-web` serves it as JSON plus an HTML dashboard:

```kotlin
val backend = LocalBackend()
ControlServer(backend, port = 8571).start()
backend.run(workflow).await()
```

Endpoints: `/` (dashboard), `/api/backend`, `/api/runs`, `/api/runs/{id}`. The Camunda backends additionally report the engine's live instance count per run.

Runs registered with `Start.Manual` wait for a trigger. The dashboard's start/pause/cancel
buttons (and the matching `POST /api/runs/{id}/start`, `/pause`, `/cancel` endpoints) call
straight through to the tracked `RunHandle`, so a separate process or operator can drive a
run that another process started.

---

## Logging

Tasks can call `log(message, "key" to value, ...)` from `loadshift-core` to record structured
log entries. Each entry automatically carries the execution tree context: run id, workflow name,
the ancestor item-key path through nested `fanOut`s, the current item key, and the current task's
topic.

Logging is opt-in via `RunConfig.logSink` (default `NoopLogSink`, which discards everything).
`loadshift-sqlite` provides `SqliteLogSink`, which persists entries to a SQLite database:

```kotlin
val sink = SqliteLogSink("logs.db")
backend.run(workflow, RunConfig(logSink = sink)).await()
```

```kotlin
task("charge") { order ->
    log("charging customer", "orderId" to order.id, "amount" to order.amount)
}
```

---

## Publishing

The library modules publish under the group `io.github.it-atelier-gn`. For local consumption:

```sh
./kotlin publish mavenLocal
```

Maven Central releases are staged from mavenLocal and uploaded with JReleaser by a separate publishing project.

---

## Documentation

The docs site lives in [docs/](docs/) and is deployed to GitHub Pages by [pages.yml](.github/workflows/pages.yml) on every push to `main` that touches `docs/`.

---

## Contributing

Contributions are welcome. For substantial changes, open an issue first to discuss the approach.

---

## License

MIT © 2026 Georg Nelles
