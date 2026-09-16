# Running on an engine

## Delivery and durability

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

## Handing over to another worker

```kotlin
val handle = Camunda8Backend(base).run(fulfilment)
handle.detach()

val takeover = Camunda8Backend(base).attach(fulfilment)
takeover.await()
```

`attach(workflow)` deploys the workflow, works its active process instances, including instances that appear while it runs, and finishes when none are left. It seeds nothing and does not accept `Start.Cron`.

## Schedules

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

## Job streaming on Camunda 8

```kotlin
val stream = CamundaJobStream(grpcAddress = URI("http://localhost:26500"), restAddress = URI("http://localhost:8080"))
val backend = Camunda8Backend("http://localhost:8080", jobStream = stream)
```

With `jobStream`, the worker opens a job stream per job type over the engine's gRPC API (port 26500 by default) and works the jobs the engine pushes before it activates jobs over REST. Jobs that existed before a stream opened are activated over REST. When a run ends, its streams close and pushed jobs that the worker has not started are handed back to the engine. `CamundaJobStream(client)` takes a configured `CamundaClient`, for example with OAuth credentials or TLS; `close()` closes a client that `CamundaJobStream` created itself.

## Versions and migration

```kotlin
val fulfilment = workflow<Order>("fulfilment") {
    version("2")
    input(orders)
    task("reserve") { reserve(it) }
}

val result = Camunda8Backend(base).migrate(fulfilment)
```

`version(tag)` sets the version tag of every process the workflow deploys (`camunda:versionTag` on Camunda 7 and CIB seven, `zeebe:versionTag` on Camunda 8). `migrate(workflow)` deploys the workflow and moves the active instances of each of its processes from older definitions to the definition just deployed. Elements are matched by id; the ids of steps stay the same while the steps before them and their topics stay the same. `MigrationResult` holds the number of migrated instances and, per process, the instances that could not be migrated with the engine's error, for example when an instance waits in an element the new version no longer has. Migrate while no worker runs the workflow, for example after `detach()`, and continue with `attach(workflow)`.

## Authentication

| Target | Configuration |
| --- | --- |
| Camunda 7, CIB seven | `Camunda7Backend(base, credentials = BasicCredentials(username, password))` |
| Camunda 8 with a token | `Camunda8Backend(base, auth = Camunda8Auth.Bearer(token))` |
| Camunda 8 with OAuth client credentials | `Camunda8Backend(base, auth = Camunda8Auth.ClientCredentials(tokenUrl, clientId, clientSecret, audience, scope))` |
| Web console with basic authentication | `ControlServer(backend, credentials = ConsoleCredentials(username, password))` |
| Web console with OpenID Connect | `ControlServer(backend, oidc = ConsoleOidc(issuer, clientId, clientSecret, redirectUrl))` |

With client credentials, the client caches the access token until 30 seconds before it expires, and requests another token after a `401` response.

## Tenants

| Engine | Configuration |
| --- | --- |
| Camunda 7, CIB seven | `Camunda7Backend(base, tenantId = "acme")` |
| Camunda 8 | `Camunda8Backend(base, auth, tenantId = "acme")` |

With `tenantId`, the backend deploys the processes to the tenant, starts instances in it, works only jobs of the tenant, correlates messages within it and searches only its instances. Camunda 8 accepts a tenant other than `<default>` when multi-tenancy is enabled and the client is authorized for that tenant.

## Process variables

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
