# Run configuration

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
| `metrics` | discards measurements | Receives counters and task durations, see [Metrics](observability.md#metrics) |

Each task also accepts `retry`, `timeout` and `rateLimit`. A task timeout counts as a failed attempt.

## Starts

| `Start` | Behaviour |
| --- | --- |
| `Start.Now` | Seeds immediately |
| `Start.Manual` | Waits for `handle.start()` |
| `Start.At(instant)` | Seeds once at the instant |
| `Start.Cron(expression, zone)` | Seeds on every matching minute in `zone` (system zone by default), after the previous batch has finished. On an engine the schedule is kept by the engine, see [Schedules](engines.md#schedules) |

Cron expressions have five fields: minute, hour, day of month, month, day of week. `Start.Cron` throws `IllegalArgumentException` for an invalid expression.

| Syntax | Example | Matches |
| --- | --- | --- |
| `*` or `?` | `*` | Every value |
| List | `1,15` | Each listed value |
| Range | `9-17`, `MON-FRI` | Every value in the range |
| Step | `*/15`, `5/20`, `8-18/4` | Every step from the start of the range |
| Names | `JAN`-`DEC`, `SUN`-`SAT` | Months and weekdays, case-insensitive |

Day of week accepts `0` and `7` for Sunday. When both day of month and day of week are restricted, a day matches if either field matches.

## Error handling

| `ErrorPolicy` | Item | Compensations | Counted as |
| --- | --- | --- | --- |
| `DeadLetter` | Ends; its parent continues | Run in reverse order | A dead letter with the item key, topic and error |
| `Skip` | Ends; its parent continues | Not run | `skipped` |
| `Fail` | The run fails and its other items are cancelled | Not run | `failed` |

Failures in conditions, loop predicates, `expand` and `reduce` follow the same policy without retries. A `timeout` block that expires and a loop that exceeds `maxLoopIterations` always produce a dead letter, with the topic `timeout_<id>` or `loop_<id>`. A compensation that throws adds a dead letter with the topic `compensate_<topic>`.

## Dead-letter store

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

## Run handle

| `RunHandle` call | Effect |
| --- | --- |
| `start()` | Starts a `Start.Manual` run |
| `pause()` | Tasks that have not started wait; running task bodies finish |
| `resume()` | Continues a paused run |
| `cancel()` | Stops the run; on an engine, its process instances are cancelled |
| `detach()` | On an engine: stops seeding and fetching, waits for running task bodies and leaves the process instances on the engine. In-process: same as `cancel()` |
| `send(message, key, data)` | Delivers the message to the item with that key once it waits for it; `data` is an optional JSON object for the step's block |
| `broadcast(message, data)` | Releases every item waiting for the message, including items that reach the wait later; `data` is an optional JSON object for the step's block |
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
