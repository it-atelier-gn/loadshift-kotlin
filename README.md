# loadshift-kotlin

[![CI](https://github.com/it-atelier-gn/loadshift-kotlin/actions/workflows/ci.yml/badge.svg)](https://github.com/it-atelier-gn/loadshift-kotlin/actions)
[![Kotlin](https://img.shields.io/badge/kotlin-2.x-blueviolet?logo=kotlin)](https://kotlinlang.org/)
[![Kotlin Toolchain](https://img.shields.io/badge/build-kotlin--toolchain-blue)](https://github.com/JetBrains/kotlin-toolchain)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Kotlin DSL for durable workflows. Define a workflow once and run it in-process, on Camunda 7, on CIB seven or on Camunda 8.

Full examples and reference: **[it-atelier-gn.github.io/loadshift-kotlin](https://it-atelier-gn.github.io/loadshift-kotlin/)**

---

## Modules

| Module | Contents |
| --- | --- |
| `loadshift-core` | The DSL, run configuration, the `Backend` API and the BPMN compiler |
| `loadshift-local` | `LocalBackend`, which runs workflows in-process |
| `loadshift-camunda-7` | `Camunda7Backend` for Camunda 7 and CIB seven, working external tasks over the REST API |
| `loadshift-camunda-8` | `Camunda8Backend` for Camunda 8, working jobs over the REST API |
| `loadshift-web` | `ControlServer`, a web console and JSON API for the runs of a backend |
| `loadshift-sqlite` | `SqliteLogSink` for `log()` entries and `SqliteCheckpointStore` for checkpoints |
| `loadshift-otel` | `OtelTracer`, which emits an OpenTelemetry span per task |
| `loadshift-demo` | A runnable example workflow |

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

Published to Maven Central under group `io.github.it-atelier-gn`, version `0.5.0`. Pick the modules you need.

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
    implementation("io.github.it-atelier-gn:loadshift-otel:0.5.0")
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
    implementation 'io.github.it-atelier-gn:loadshift-otel:0.5.0'
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
<dependency>
    <groupId>io.github.it-atelier-gn</groupId>
    <artifactId>loadshift-otel</artifactId>
    <version>0.5.0</version>
</dependency>
```
</details>

---

## Requirements

- JDK 25 or newer. The libraries are compiled for Java 25.
- Docker, for the end-to-end tests and the local dev engines.

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

### Steps

| Step | In-process | On an engine |
| --- | --- | --- |
| `task(topic) { }` | Runs the body with retry, timeout and rate limit | Service task with the job type `<workflow-key>/<topic>` |
| `condition(predicate) { } otherwise { }` | Runs the matching branch | Service task and exclusive gateways |
| `loop(predicate) { }` | Repeats the body while the predicate holds | Service task and a gateway loop |
| `parallel { branch { } }` | Runs the branches concurrently | Parallel gateways |
| `fanOut(expand) { }` | Runs the body per child, `concurrency` at a time | Child process called by a parallel multi-instance call activity |
| `.reduce(initial, combine) { }`, `.collect { }` | Folds the expanded children into the parent | Service task after the call activity |
| `wait(duration)` | Suspends the item | Timer catch event |
| `timeout(duration) { }` | Cancels the block and dead-letters the item | Embedded subprocess with an interrupting boundary timer and a service task that records the dead letter |
| `awaitMessage(name)` | Waits for `send` or `broadcast` | Message catch event |
| `task(...) { } compensate { }` | Runs when the item is dead-lettered, in reverse order | Same, from a snapshot stored as a process variable |

A workflow name becomes its key: lowercased, with every character other than letters, digits, `_` and `-` replaced by `-`. The key must start with a letter or `_`. Topics must be unique within a workflow, and the names `decision_c<n>`, `decision_l<n>`, `expand_f<n>`, `reduce_f<n>`, `timeout_to<n>` and `loop_l<n>` are reserved.

---

## Run configuration

| `RunConfig` option | Default | Effect |
| --- | --- | --- |
| `start` | `Start.Now` | When seeding begins, see [Starts](#starts) |
| `maxConcurrency` | `16` | In-process: items processed at once. On an engine: task bodies the worker runs at once |
| `rateLimit` | none | Task executions per time window, per worker |
| `retry` | 3 attempts, 200 ms base delay, 30 s cap, jitter | Retry policy for tasks that set none |
| `onError` | `ErrorPolicy.DeadLetter` | What happens to an item whose task exhausted its attempts, see [Error handling](#error-handling) |
| `dryRun` | `false` | In-process: evaluates the flow without running task bodies. On an engine: compiles the BPMN, deploys nothing and starts nothing |
| `dedupe` | `false` | Skips seeded items whose key was already seeded in the same run |
| `checkpoints` | none | Store of completed item keys per workflow (`InMemoryCheckpointStore`, `SqliteCheckpointStore`) |
| `resume` | `true` | Skips items the checkpoint store lists as complete |
| `lockDuration` | 5 minutes | Engine lock per job |
| `maxLoopIterations` | `10000` | Iterations after which a loop dead-letters the item |
| `logSink` | discards entries | Receives `log()` entries |
| `tracer` | none | Receives one span per task |

Each task also accepts `retry`, `timeout` and `rateLimit`. A task timeout counts as a failed attempt.

### Starts

| `Start` | Behaviour |
| --- | --- |
| `Start.Now` | Seeds immediately |
| `Start.Manual` | Waits for `handle.start()` |
| `Start.At(instant)` | Seeds once at the instant |
| `Start.Cron(expression, zone)` | Seeds on every matching minute in `zone` (system zone by default), after the previous batch has finished |

Cron expressions have five fields: minute, hour, day of month, month, day of week. `Start.Cron` throws `IllegalArgumentException` for an invalid expression.

| Syntax | Example | Matches |
| --- | --- | --- |
| `*` or `?` | `*` | Every value |
| List | `1,15` | Each listed value |
| Range | `9-17`, `MON-FRI` | Every value in the range |
| Step | `*/15`, `5/20`, `8-18/4` | Every step from the start of the range |
| Names | `JAN`-`DEC`, `SUN`-`SAT` | Months and weekdays, case-insensitive |

Day of week accepts `0` and `7` for Sunday. When both day of month and day of week are restricted, a day matches if either field matches.

### Error handling

| `ErrorPolicy` | Item | Compensations | Counted as |
| --- | --- | --- | --- |
| `DeadLetter` | Ends; its parent continues | Run in reverse order | A dead letter with the item key, topic and error |
| `Skip` | Ends; its parent continues | Not run | `skipped` |
| `Fail` | The run fails and its other items are cancelled | Not run | `failed` |

Failures in conditions, loop predicates, `expand` and `reduce` follow the same policy without retries. A `timeout` block that expires and a loop that exceeds `maxLoopIterations` always produce a dead letter, with the topic `timeout_<id>` or `loop_<id>`. A compensation that throws adds a dead letter with the topic `compensate_<topic>`.

### Run handle

| `RunHandle` call | Effect |
| --- | --- |
| `start()` | Starts a `Start.Manual` run |
| `pause()` | Tasks that have not started wait; running task bodies finish |
| `resume()` | Continues a paused run |
| `cancel()` | Stops the run; on an engine, its process instances are cancelled |
| `detach()` | On an engine: stops seeding and fetching, waits for running task bodies and leaves the process instances on the engine. In-process: same as `cancel()` |
| `send(message, key)` | Delivers the message to the item with that key once it waits for it |
| `broadcast(message)` | Releases every item waiting for the message, including items that reach the wait later |
| `progress()` | Current counters |
| `await()` | The `RunResult`; throws the failure of a run that failed under `ErrorPolicy.Fail` |

| Counter | Counts |
| --- | --- |
| `seeded` | Items started from `input`, plus instances taken over by `attach` |
| `expanded` | Children produced by fan-outs |
| `done` | Top-level items that reached the end |
| `failed` | Items that failed the run under `ErrorPolicy.Fail` |
| `skipped` | Items skipped by `dedupe`, by checkpoints or by `ErrorPolicy.Skip` |

---

## Running on an engine

### Delivery and durability

- Task bodies run at least once. The engine delivers a job again when the worker stops before completing it, when the job's lock expires, or when the completion call fails five times. Write task bodies so that running them twice has the same effect as running them once.
- While a task body runs, the worker extends the job's lock every third of `lockDuration`.
- A worker that stops hands back the jobs it fetched while stopping.
- Retries are counted by the engine. Each failure reports the remaining attempts and the backoff. Camunda 8 service tasks are deployed with `retries` set to the task's `maxAttempts`.
- Every compiled process contains the event subprocess `on_terminate`, which catches the BPMN error `loadshift-terminate`. Dead-lettered and skipped items end through it, so the instance completes and its parent continues.
- When a task with `compensate` completes, the worker stores a snapshot of the item in the process variable `loadshiftCompensation_<topic>`. Any worker that dead-letters the item runs the compensations from these snapshots, latest first. A task that runs several times in one instance keeps the snapshot of its last run.
- `send` and `broadcast` reach waiting instances of the same workflow, whichever run started them. A `send` that finds no waiting instance is retried until one waits or the run ends.
- A run finishes when every root process instance it started or took over has finished. `ErrorPolicy.Fail` cancels those instances.
- Dead letters, counters and the run result are held by the process that holds the `RunHandle`.

### Handing over to another worker

```kotlin
val handle = Camunda8Backend(base).run(fulfilment)
handle.detach()

val takeover = Camunda8Backend(base).attach(fulfilment)
takeover.await()
```

`attach(workflow)` deploys the workflow, works its active process instances, including instances that appear while it runs, and finishes when none are left. It seeds nothing and does not accept `Start.Cron`.

### Authentication

| Target | Configuration |
| --- | --- |
| Camunda 7, CIB seven | `Camunda7Backend(base, credentials = BasicCredentials(username, password))` |
| Camunda 8 with a token | `Camunda8Backend(base, auth = Camunda8Auth.Bearer(token))` |
| Camunda 8 with OAuth client credentials | `Camunda8Backend(base, auth = Camunda8Auth.ClientCredentials(tokenUrl, clientId, clientSecret, audience, scope))` |
| Web console | `ControlServer(backend, credentials = ConsoleCredentials(username, password))` |

With client credentials, the client caches the access token until 30 seconds before it expires, and requests another token after a `401` response.

### Process variables

| Variable | Instance | Content |
| --- | --- | --- |
| Fields of the root item | Root | The seeded item |
| `loadshiftKey` | Root and child | The item key |
| `loadshiftWorkflow` | Root and child | The workflow key |
| `<fan-out id>_items` | Parent | The expanded children |
| `<fan-out id>_item` | Child | The child item, with `loadshiftKey` and `loadshiftParents` |
| `<step id>_result` | Any | The result of a condition or loop predicate |
| `<loop id>_iterations` | Any | Iterations of the current loop |
| `loadshiftCompensation_<topic>` | Any | Item snapshot for compensation |
| `loadshiftOutcome` | Any | Policy, topic and error of a dead-lettered or skipped item |

Camunda 8 correlates messages on the key `<workflow-key>:<item-key>`. Camunda 7 and CIB seven correlate on the variables `loadshiftWorkflow` and `loadshiftKey`. Camunda 7 and CIB seven receive the item key as business key when it is at most 255 characters long.

### Not supported

Tenant IDs, user tasks, signal events and metrics export.

---

## Web console

```kotlin
val backend = Camunda8Backend("http://localhost:8080")
ControlServer(backend, port = 8571, credentials = ConsoleCredentials("ops", secret)).start()
```

The server listens on `127.0.0.1` unless `host` is set. With `credentials`, every page and endpoint requires HTTP basic authentication.

| Method | Path | Effect |
| --- | --- | --- |
| `GET` | `/` | Dashboard |
| `GET` | `/api/backend` | Backend type and number of runs |
| `GET` | `/api/runs` | State, counters and dead letters of every run |
| `GET` | `/api/runs/{id}` | One run with its flow structure |
| `POST` | `/api/runs/{id}/start` | `start()` |
| `POST` | `/api/runs/{id}/pause` | `pause()` |
| `POST` | `/api/runs/{id}/resume` | `resume()` |
| `POST` | `/api/runs/{id}/cancel` | `cancel()` |
| `POST` | `/api/runs/{id}/detach` | `detach()` |

Unknown run ids return `404`. For engine backends, each run also shows the number of active process instances of its workflow on the engine.

---

## Logging and tracing

```kotlin
task("charge") { order ->
    log("charging customer", "orderId" to order.id, "amount" to order.total)
}

backend.run(workflow, RunConfig(logSink = SqliteLogSink("logs.db"), tracer = OtelTracer(openTelemetry)))
```

Each log entry carries the run id, workflow name, the keys of the parent items, the item key and the task topic.

---

## End-to-end tests

| Suite | Container image | Variable for an engine that is already running |
| --- | --- | --- |
| `Camunda7E2eTest` | `camunda/camunda-bpm-platform:run-7.24.0` | `LOADSHIFT_C7_BASE=http://localhost:8080/engine-rest` |
| `CibSevenE2eTest` | `cibseven/cibseven:run-2.2.0` | `LOADSHIFT_CIB7_BASE=http://localhost:8080/engine-rest` |
| `Camunda8E2eTest` | `camunda/camunda:8.9.19` with H2 secondary storage | `LOADSHIFT_C8_BASE=http://localhost:8080` |

Without Docker and without the variable, a suite is reported as skipped.

## Local dev engines

| Command | Engine |
| --- | --- |
| `./kotlin run -m engine c7` | Camunda 7 Run on port 8080, Cockpit at `/camunda` (demo/demo) |
| `./kotlin run -m engine cib7` | CIB seven on port 8080, Cockpit at `/camunda/app/` (demo/demo) |
| `./kotlin run -m engine c8` | Camunda 8 on port 8080, Operate at `/operate` (demo/demo), H2 storage |
| `./kotlin run -m engine c8 logs` | Follows the engine log |
| `./kotlin run -m engine c8 stop` | Stops the engine |
| `./kotlin run -m engine c7 start 9090` | Starts the engine on another port |

The command uses Podman when it is installed and Docker otherwise. Run it from the repository root.

## BPMN examples

| Command | Effect |
| --- | --- |
| `./kotlin run -m examples` | Verifies the example workflows for both dialects and writes [docs/examples.html](https://it-atelier-gn.github.io/loadshift-kotlin/examples.html) |
| `./kotlin run -m examples verify` | Verifies without writing the page |

## Dependency check

| Command | Effect |
| --- | --- |
| `./kotlin run -m deps` | Lists the libraries of `libs.versions.toml` and the Kotlin Toolchain with their latest stable versions |
| `./kotlin run -m deps -- check` | Same; exits with `1` when an update is available and `2` when a lookup failed |

The [Dependencies](.github/workflows/dependencies.yml) workflow runs the check every Monday.

## Publishing

The library modules publish under the group `io.github.it-atelier-gn`. The group and version are set in [templates/library.module-template.yaml](templates/library.module-template.yaml).

```sh
./kotlin publish mavenLocal
```

## Documentation

The docs site lives in [docs/](docs/) and is deployed to GitHub Pages by [pages.yml](.github/workflows/pages.yml) on every push to `main` that touches `docs/`.

## Contributing

Contributions are welcome. For substantial changes, open an issue first to discuss the approach.

## License

MIT, see [LICENSE](LICENSE).
