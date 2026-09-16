# loadshift-kotlin

[![Maven Central](https://img.shields.io/maven-central/v/io.github.it-atelier-gn/loadshift-core)](https://central.sonatype.com/namespace/io.github.it-atelier-gn)
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
| `loadshift-camunda-8-stream` | `CamundaJobStream`, which lets `Camunda8Backend` receive jobs the engine pushes over gRPC, see [Job streaming](#job-streaming-on-camunda-8) |
| `loadshift-web` | `ControlServer`, a web console and JSON API for the runs of a backend |
| `loadshift-sqlite` | `SqliteLogSink` for `log()` entries, `SqliteCheckpointStore` for checkpoints, `SqliteDeadLetterStore` for dead letters and `SqliteRunRegistry` for runs of several workers |
| `loadshift-otel` | `OtelTracer`, which emits an OpenTelemetry span per task, and `OtelMetrics`, which reports [metrics](#metrics) through OpenTelemetry |
| `loadshift-micrometer` | `MicrometerMetrics`, which reports [metrics](#metrics) to a Micrometer `MeterRegistry` |
| `loadshift-demo` | A runnable example workflow |
| `loadshift-bench` | A load test that measures throughput and item latency of a backend, see [Load tests](#load-tests) |

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
| `awaitSignal(name)` | Waits for `backend.signal(name)` | Signal catch event |
| `userTask(name, assignee, candidateGroups) { item, form -> }` | Waits until `completeUserTask` is called, then runs the block with the form | User task with its assignment, followed by a service task that runs the block |
| `call(workflow)` | Runs the steps of the other workflow on the item | Service task that hands over the item, call activity of the other workflow's process, service task that writes the item back |
| `task(...) { } compensate { }` | Runs when the item is dead-lettered, in reverse order | Same, from a snapshot stored as a process variable |

`backend.signal(name)` releases every item that waits for the signal at that moment, in every workflow: in-process in all active runs of the `LocalBackend`, on an engine in all process instances of the engine (of the backend's tenant when `tenantId` is set). An item that reaches `awaitSignal` afterwards waits for the next signal.

`backend.userTasks(workflow)` returns the open user tasks of the workflow and of the workflows it calls, each with its id, name, item key, assignee and candidate groups. `backend.completeUserTask(id, form)` completes one with a JSON object and returns `false` for a task that is not open; the block of the step applies the form to the item. On an engine these are the engine's user tasks (Camunda user tasks on Camunda 8), so any task list of the engine can complete them as well when it sets the variable `<step id>_form`.

`call(workflow)` takes a workflow with the same item type; its `input` is not used. Changes the called steps make to the item are visible to the caller afterwards. A dead letter or skip inside the called steps ends them, is recorded under the called workflow's key and level, and the caller continues with the next step. On an engine the run deploys the called workflow together with the caller and works its jobs, and `send` and `broadcast` reach items waiting inside called workflows.

A workflow name becomes its key: lowercased, with every character other than letters, digits, `_` and `-` replaced by `-`. The key must start with a letter or `_`. Topics must be unique within a workflow, and the names `decision_c<n>`, `decision_l<n>`, `expand_f<n>`, `reduce_f<n>`, `timeout_to<n>`, `loop_l<n>`, `call_cw<n>`, `return_cw<n>` and `form_ut<n>` are reserved.

---

## Run configuration

| `RunConfig` option | Default | Effect |
| --- | --- | --- |
| `start` | `Start.Now` | When seeding begins, see [Starts](#starts) |
| `maxConcurrency` | `16` | In-process: items processed at once. On an engine: task bodies the worker runs at once |
| `maxInFlight` | none | Top-level items a run keeps unfinished at once. On an engine: root process instances the run has started and that have not finished; seeding and requeueing wait until one finishes. Instances taken over by `attach` do not count |
| `rateLimit` | none | Task executions per time window, per worker |
| `retry` | 3 attempts, 200 ms base delay, 30 s cap, jitter | Retry policy for tasks that set none |
| `onError` | `ErrorPolicy.DeadLetter` | What happens to an item whose task exhausted its attempts, see [Error handling](#error-handling) |
| `dryRun` | `false` | In-process: evaluates the flow without running task bodies. On an engine: compiles the BPMN, deploys nothing and starts nothing |
| `dedupe` | `false` | Skips seeded items whose key was already seeded in the same run |
| `checkpoints` | none | Store of completed item keys per workflow (`InMemoryCheckpointStore`, `SqliteCheckpointStore`) |
| `resume` | `true` | Skips items the checkpoint store lists as complete |
| `deadLetters` | none | Store that receives every dead letter with the item and its workflow level (`InMemoryDeadLetterStore`, `SqliteDeadLetterStore`), see [Dead-letter store](#dead-letter-store) |
| `lockDuration` | 5 minutes | Engine lock per job |
| `maxLoopIterations` | `10000` | Iterations after which a loop dead-letters the item |
| `logSink` | discards entries | Receives `log()` entries |
| `tracer` | none | Receives one span per task |
| `metrics` | discards measurements | Receives counters and task durations, see [Metrics](#metrics) |

Each task also accepts `retry`, `timeout` and `rateLimit`. A task timeout counts as a failed attempt.

### Starts

| `Start` | Behaviour |
| --- | --- |
| `Start.Now` | Seeds immediately |
| `Start.Manual` | Waits for `handle.start()` |
| `Start.At(instant)` | Seeds once at the instant |
| `Start.Cron(expression, zone)` | Seeds on every matching minute in `zone` (system zone by default), after the previous batch has finished. On an engine the schedule is kept by the engine, see [Schedules](#schedules) |

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

### Dead-letter store

```kotlin
val store = SqliteDeadLetterStore("dead-letters.db")
backend.run(fulfilment, RunConfig(deadLetters = store)).await()

val page = store.list(fulfilment.key, limit = 100)
backend.requeue(fulfilment, page.records, RunConfig(deadLetters = store)).await()
```

| `DeadLetterRecord` field | Content |
| --- | --- |
| `id` | Unique id of the record |
| `workflowKey` | Key of the workflow |
| `level` | Process id of the level the item belongs to: the workflow key or a fan-out child process |
| `itemVariable` | `null` for top-level items, `<fan-out id>_item` for children |
| `deadLetter` | Item key, topic and error |
| `item` | The item as JSON, with `loadshiftKey` and, for children, `loadshiftParents` |
| `recordedAt` | Time of recording, stored by `SqliteDeadLetterStore` with millisecond precision |
| `runId` | Id of the run that recorded the dead letter, the same id its log entries carry |

`store.forRun(runId, limit, after)` returns the records of one run in the same pages.

`store.list(workflowKey, limit, after)` returns records ordered by recording time and a `nextCursor` for the following page. Removing records between pages neither repeats nor skips a record. `store.forKey(workflowKey, itemKey)` returns every record of one item, so `backend.requeue(fulfilment, store.forKey(fulfilment.key, "order-17"), config)` retries that item.

`backend.requeue(workflow, records, config)` runs each item again from the start of its level: a top-level item runs the whole workflow, a fan-out child runs the child's steps with its parents available through `context()`. A requeued child does not run its parent's `reduce` or `collect` again. Each record is removed from `config.deadLetters` once its item has started; an item that fails again is recorded again. A requeue run counts every requeued item that completes as `done` and does not accept `Start.Cron`.

With `deadLetters` set, the in-process backend encodes items with their serializer to build records, so work items need `@Serializable`.

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
| `item(key)` | `ItemStatus` of the top-level item with that key: state, current topic and dead letters; `null` for a key the run does not know |
| `cancelItem(key)` | Stops the top-level item with that key without running its compensations; on an engine, its root process instance is cancelled. Returns `false` when the item is not waiting or running |
| `progress()` | Current counters |
| `await()` | The `RunResult`; throws the failure of a run that failed under `ErrorPolicy.Fail` |

| `ItemState` | Item |
| --- | --- |
| `Waiting` | In-process: seeded and waiting for a free slot of `maxConcurrency` |
| `Running` | In-process: its steps run. On an engine: its root process instance is active |
| `Done` | Reached the end |
| `DeadLettered` | Ended with a dead letter |
| `Skipped` | Ended through `ErrorPolicy.Skip` |
| `Cancelled` | Stopped by `cancelItem` |

A run keeps the state of the 10,000 most recently finished items. The current topic is reported by the in-process backend; on an engine it is `null`.

| Counter | Counts |
| --- | --- |
| `seeded` | Items started from `input`, plus instances taken over by `attach` |
| `expanded` | Children produced by fan-outs |
| `done` | Top-level items that reached the end |
| `failed` | Items that failed the run under `ErrorPolicy.Fail` |
| `skipped` | Items skipped by `dedupe`, by checkpoints or by `ErrorPolicy.Skip` |
| `cancelled` | Items stopped by `cancelItem` |

---

## Running on an engine

### Delivery and durability

- Task bodies run at least once. The engine delivers a job again when the worker stops before completing it, when the job's lock expires, or when the completion call fails five times. Write task bodies so that running them twice has the same effect as running them once.
- While a task body runs, the worker extends the job's lock every third of `lockDuration`.
- A worker runs a job at most once at a time. When the engine hands the same job to that worker again while its task body is still running, the worker skips the second copy.
- A worker that stops hands back the jobs it fetched while stopping.
- Retries are counted by the engine. Each failure reports the remaining attempts and the backoff. Camunda 8 service tasks are deployed with `retries` set to the task's `maxAttempts`.
- Every compiled process contains the event subprocess `on_terminate`, which catches the BPMN error `loadshift-terminate`. Dead-lettered and skipped items end through it, so the instance completes and its parent continues.
- When a task with `compensate` completes, the worker stores a snapshot of the item in the process variable `loadshiftCompensation_<topic>`. Any worker that dead-letters the item runs the compensations from these snapshots, latest first. A task that runs several times in one instance, such as inside a loop, adds one snapshot per run to the variable, and each snapshot is compensated.
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

### Schedules

A run with `Start.Cron` deploys the process `<workflow-key>_loadshift_schedule` together with the workflow and starts one instance of it, or continues the instance that is already active. The instance repeats three steps:

| Step | Effect |
| --- | --- |
| `loadshift_schedule_next` | The worker computes the next matching minute in `zone`; a timer on the engine waits for it |
| `loadshift_schedule_seed` | The worker seeds the batch |
| `loadshift_schedule_await` | The worker waits until the root instances of the batch have finished |

- A tick that passes while no worker runs is seeded once a worker with `Start.Cron` runs again. The worker takes over the active root instances of the workflow first and waits for them.
- An item whose key belongs to an active root instance is skipped and counted as `skipped`.
- The schedule steps do not use `maxConcurrency` slots.
- `cancel()` cancels the schedule instance. `detach()` leaves it on the engine.
- Run one worker with `Start.Cron` per workflow at a time.

### Job streaming on Camunda 8

```kotlin
val stream = CamundaJobStream(grpcAddress = URI("http://localhost:26500"), restAddress = URI("http://localhost:8080"))
val backend = Camunda8Backend("http://localhost:8080", jobStream = stream)
```

With `jobStream`, the worker opens a job stream per job type over the engine's gRPC API (port 26500 by default) and works the jobs the engine pushes before it activates jobs over REST. Jobs that existed before a stream opened are activated over REST. When a run ends, its streams close and pushed jobs that the worker has not started are handed back to the engine. `CamundaJobStream(client)` takes a configured `CamundaClient`, for example with OAuth credentials or TLS; `close()` closes a client that `CamundaJobStream` created itself.

### Versions and migration

```kotlin
val fulfilment = workflow<Order>("fulfilment") {
    version("2")
    input(orders)
    task("reserve") { reserve(it) }
}

val result = Camunda8Backend(base).migrate(fulfilment)
```

`version(tag)` sets the version tag of every process the workflow deploys (`camunda:versionTag` on Camunda 7 and CIB seven, `zeebe:versionTag` on Camunda 8). `migrate(workflow)` deploys the workflow and moves the active instances of each of its processes from older definitions to the definition just deployed. Elements are matched by id; the ids of steps stay the same while the steps before them and their topics stay the same. `MigrationResult` holds the number of migrated instances and, per process, the instances that could not be migrated with the engine's error, for example when an instance waits in an element the new version no longer has. Migrate while no worker runs the workflow, for example after `detach()`, and continue with `attach(workflow)`.

### Authentication

| Target | Configuration |
| --- | --- |
| Camunda 7, CIB seven | `Camunda7Backend(base, credentials = BasicCredentials(username, password))` |
| Camunda 8 with a token | `Camunda8Backend(base, auth = Camunda8Auth.Bearer(token))` |
| Camunda 8 with OAuth client credentials | `Camunda8Backend(base, auth = Camunda8Auth.ClientCredentials(tokenUrl, clientId, clientSecret, audience, scope))` |
| Web console with basic authentication | `ControlServer(backend, credentials = ConsoleCredentials(username, password))` |
| Web console with OpenID Connect | `ControlServer(backend, oidc = ConsoleOidc(issuer, clientId, clientSecret, redirectUrl))` |

With client credentials, the client caches the access token until 30 seconds before it expires, and requests another token after a `401` response.

### Tenants

| Engine | Configuration |
| --- | --- |
| Camunda 7, CIB seven | `Camunda7Backend(base, tenantId = "acme")` |
| Camunda 8 | `Camunda8Backend(base, auth, tenantId = "acme")` |

With `tenantId`, the backend deploys the processes to the tenant, starts instances in it, works only jobs of the tenant, correlates messages within it and searches only its instances. Camunda 8 accepts a tenant other than `<default>` when multi-tenancy is enabled and the client is authorized for that tenant.

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
| `loadshiftCompensation_<topic>` | Any | Item snapshots for compensation, one per run of the task |
| `loadshiftOutcome` | Any | Policy, topic and error of a dead-lettered or skipped item |
| `<call id>_call` | Caller | The item handed to a called workflow and returned from it |
| `loadshiftCallItem` | Called root | The item of a called workflow, with `loadshiftKey` |

Camunda 8 correlates messages on the key `<workflow-key>:<item-key>`. Camunda 7 and CIB seven correlate on the variables `loadshiftWorkflow` and `loadshiftKey`. Camunda 7 and CIB seven receive the item key as business key when it is at most 255 characters long.

---

## Web console

```kotlin
val backend = Camunda8Backend("http://localhost:8080")
ControlServer(backend, port = 8571, credentials = ConsoleCredentials("ops", secret)).start()
```

The server listens on `127.0.0.1` unless `host` is set. With `credentials`, every page and endpoint requires HTTP basic authentication. With `oidc`, the console signs users in through an OpenID Connect provider:

```kotlin
val oidc = ConsoleOidc(
    issuer = "https://login.example.com/realms/ops",
    clientId = "loadshift-console",
    clientSecret = secret,
    redirectUrl = "https://console.example.com/login/callback",
    requiredClaims = mapOf("groups" to "loadshift-operators"),
)
ControlServer(backend, host = "0.0.0.0", oidc = oidc).start()
```

| `ConsoleOidc` option | Default | Effect |
| --- | --- | --- |
| `issuer` | required | Issuer URL; the server reads `<issuer>/.well-known/openid-configuration` when it starts |
| `clientId`, `clientSecret` | required | Confidential client registered at the provider |
| `redirectUrl` | required | Callback URL registered at the provider; its path serves the callback |
| `scopes` | `openid`, `profile`, `email` | Requested scopes; must contain `openid` |
| `requiredClaims` | none | Claims a user's token must carry; a claim matches when it equals the value or is a list containing it |
| `sessionTtl` | 8 hours | Lifetime of a console session |
| `secureCookie` | `true` | Sends the session cookie only over HTTPS |
| `sessionKey` | random per start | At least 32 bytes; set the same key on every console instance so sessions survive restarts and work across instances |

Pages without a session redirect to `/login`, which forwards to the provider. The provider returns to `redirectUrl`; the server verifies the ID token (RS256 signature from the provider's JWKS, issuer, audience `clientId`, expiry), checks `requiredClaims` (`403` otherwise) and sets an encrypted, signed, HttpOnly session cookie. `/logout` ends the session. API calls without a session accept `Authorization: Bearer <token>` with a token that passes the same checks and return `401` otherwise.

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
| `GET` | `/api/runs/{id}/items/{key}` | `item(key)`; `404` for an unknown key |
| `POST` | `/api/runs/{id}/items/{key}/cancel` | `cancelItem(key)`; `404` when the item is not waiting or running |
| `GET` | `/api/runs/{id}/logs?limit=<n>&after=<cursor>` | One page of the run's log entries from `logs`, `limit` between 1 and 500 (default 50) |
| `GET` | `/api/runs/{id}/dead-letters?limit=<n>&after=<cursor>` | One page of the run's dead-letter records with their items, `limit` between 1 and 500 (default 50) |
| `GET` | `/api/dead-letters/workflows` | Workflows whose dead letters the console shows |
| `GET` | `/api/dead-letters?workflow=<key>&limit=<n>&after=<cursor>` | One page of dead-letter records, `limit` between 1 and 500 (default 50) |
| `POST` | `/api/dead-letters/{id}/requeue` | Requeues the record; the requeue run appears under `/api/runs` |
| `POST` | `/api/dead-letters/requeue?workflow=<key>&item=<item-key>` | Requeues every record of the item; `404` when there is none |
| `GET` | `/api/user-tasks` | Open user tasks of the workflows passed as `userTasks` |
| `POST` | `/api/user-tasks/{id}/complete` | Completes the task with the JSON object in the body; `404` when it is not open, `400` for a body that is not a JSON object |
| `DELETE` | `/api/dead-letters/{id}` | Discards the record |

The dead-letter endpoints exist when the server is created with `deadLetters = DeadLetterConsole(store, workflows, requeueConfig)` and return `404` otherwise. Requeues use `requeueConfig` with `deadLetters` set to the store.

The user task endpoints exist when the server is created with `userTasks = listOf(workflow, ...)` and return `404` otherwise.

The log endpoint exists when the server is created with `logs`, a `LogReader` such as the `SqliteLogSink` or `InMemoryLogSink` the runs write to, and returns `404` otherwise. The dashboard shows both per run.

Unknown run ids and record ids return `404`. For engine backends, each run also shows the number of active process instances of its workflow on the engine.

### Runs of several workers

```kotlin
val registry = SqliteRunRegistry("runs.db")
val backend = Camunda8Backend(base, registry = registry)
```

With a `RunRegistry` (`LocalBackend(registry)`, `Camunda7Backend(base, registry = registry)`, `Camunda8Backend(base, registry = registry)`), the backend writes the state, counters and number of dead letters of each of its runs to the registry every 5 seconds and once more when the run ends. Run ids then have the form `<worker>:run-<n>`. `/api/runs` lists the runs of the server's backend together with the records of other workers:

| Field in `/api/runs` | Content |
| --- | --- |
| `worker` | Name of the worker that holds the run |
| `controllable` | `false` for runs of other workers; their commands and `/api/runs/{id}` return `404` |
| `deadLetterCount` | Number of dead letters of the run |
| `updatedAt` | Time of the last record of another worker's run |
| `stale` | `true` for an unfinished run whose worker has not written a record for 15 seconds |

`SqliteRunRegistry` serves workers on one host. Implement `RunRegistry` to share run records across hosts.

---

## Logging and tracing

```kotlin
task("charge") { order ->
    log("charging customer", "orderId" to order.id, "amount" to order.total)
}

backend.run(workflow, RunConfig(logSink = SqliteLogSink("logs.db"), tracer = OtelTracer(openTelemetry)))
```

Each log entry carries the run id, workflow name, the keys of the parent items, the item key and the task topic.

`SqliteLogSink` and `InMemoryLogSink` also implement `LogReader`: `list(runId, limit, after)` returns the entries of one run in writing order, with a cursor for the next page. `InMemoryLogSink` keeps the 10,000 most recent entries.

### Metrics

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

---

## End-to-end tests

| Suite | Container image | Variable for another image | Variable for an engine that is already running |
| --- | --- | --- | --- |
| `Camunda7E2eTest` | `camunda/camunda-bpm-platform:run-7.24.0` | `LOADSHIFT_C7_IMAGE` | `LOADSHIFT_C7_BASE=http://localhost:8080/engine-rest` |
| `CibSevenE2eTest` | `cibseven/cibseven:run-2.2.0` | `LOADSHIFT_CIB7_IMAGE` | `LOADSHIFT_CIB7_BASE=http://localhost:8080/engine-rest` |
| `Camunda8E2eTest` | `camunda/camunda:8.9.19` with H2 secondary storage | `LOADSHIFT_C8_IMAGE` | `LOADSHIFT_C8_BASE=http://localhost:8080` |

Without Docker and without the base variable, a suite is reported as skipped. The workflow `Engine versions` runs the suites against these engine releases:

| Engine | Images |
| --- | --- |
| Camunda 7 | `camunda/camunda-bpm-platform:run-7.23.0`, `run-7.24.0` |
| CIB seven | `cibseven/cibseven:run-2.1.0`, `run-2.2.0` |
| Camunda 8 | `camunda/camunda:8.9.19` |

## Load tests

```sh
./kotlin run -m loadshift-bench -- --backend local --items 10000 --tasks 3 --concurrency 64
./kotlin run -m loadshift-bench -- --backend camunda8 --base http://localhost:8080 --items 1000 --work 20
```

| Option | Default | Effect |
| --- | --- | --- |
| `--backend` | `local` | `local`, `camunda7` or `camunda8` |
| `--base` | `http://localhost:8080/engine-rest` for `camunda7`, `http://localhost:8080` for `camunda8` | REST address of the engine |
| `--items` | `1000` | Items seeded |
| `--tasks` | `3` | Tasks per item |
| `--work` | `0` | Milliseconds each task body waits |
| `--concurrency` | `16` | `RunConfig.maxConcurrency` |

The run prints a table with the items per second and the 50th, 95th and 99th percentile of the time from seeding an item to the end of its last task. It exits with `1` when not every item finished and with `2` for invalid options. The [local dev engines](#local-dev-engines) serve as targets for `camunda7` and `camunda8`.

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

To try local changes in another project, publish the library modules to the local Maven repository. They publish under the group `io.github.it-atelier-gn`. The group and version are set in [templates/library.module-template.yaml](templates/library.module-template.yaml).

```sh
./kotlin publish mavenLocal \
  -m loadshift-core -m loadshift-local \
  -m loadshift-camunda-7 -m loadshift-camunda-8 -m loadshift-camunda-8-stream \
  -m loadshift-web -m loadshift-sqlite -m loadshift-otel -m loadshift-micrometer
```

## Documentation

The docs site lives in [docs/](docs/) and is deployed to GitHub Pages by [pages.yml](.github/workflows/pages.yml) on every push to `main` that touches `docs/`.

## Contributing

Contributions are welcome. For substantial changes, open an issue first to discuss the approach.

## License

MIT, see [LICENSE](LICENSE).
