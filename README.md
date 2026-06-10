# loadshift-kotlin

Status: under active development.

A Kotlin DSL for batch, cleanup-style durable execution. You describe a per-item flow once and run
the same workflow in-process for testing or durably on Camunda 7 / Camunda 8.

The core model is hierarchical fan-out: a seed list (for example millions of customer ids) where each
item expands into children (for example contacts), and a unit of work runs per child. Fan-out nests
to any depth.

## Example

```kotlin
class Customer(v: MutableMap<String, Any?> = mutableMapOf()) : WorkItemBase(v) {
    var id: String by required(variables)
}
class Contact(v: MutableMap<String, Any?> = mutableMapOf()) : WorkItemBase(v) {
    var id: String by required(variables)
    var email: String? by optional(variables)
}

val cleanup = workflow<Customer>("contact-cleanup") {
    key { it.id }
    items(customers)                                         // or source { Flow } for millions
    forEach<Contact>(expand = { fetchContacts(it.id) }, concurrency = 4) {
        ifThen({ it.email == null }) {
            task("flag-missing") { flag(it) }
        } elseThen {
            task("cleanup", TaskOptions(retry = RetryPolicy(maxAttempts = 3))) { clean(it) }
        }
    }
}

LocalBackend().run(cleanup).await()                          // in-process, no infrastructure
Camunda7Backend("http://localhost:8080/engine-rest").run(cleanup).await()
```

## Features

- Fan-out DSL: `forEach(expand) { }`, nestable to any depth. Child type is inferred, no factory needed.
- Control flow: `ifThen` / `elseThen`, `whileLoop`, `parallel { branch { } }`. Predicates stay Kotlin
  lambdas on every backend; they are never translated to FEEL or JUEL.
- Streaming seed: `items(list)` for small batches, `source { Flow }` or `source(paginate { })` for
  millions of items without materializing them. Bounded by backpressure.
- Resilience: per-task `RetryPolicy` (backoff, jitter, classification, timeout) and `ErrorPolicy`
  (DeadLetter, Fail, Skip) with an exportable dead-letter set.
- Throughput: `maxConcurrency`, `rateLimit`, per-level `concurrency`.
- Start modes: `Start.Now`, `Start.Manual` (fire via `RunHandle.start()`), `Start.At(instant)`,
  `Start.Cron(expr)`.
- Dry run: `LocalBackend().dryRun(wf)` returns the executed topics without side effects.

## Modules

| Module | Description |
| --- | --- |
| `loadshift-core` | Flow IR, the DSL, the `Backend` SPI, and the BPMN compiler. |
| `loadshift-local` | In-process interpreter (`LocalBackend`) for tests and development. |
| `loadshift-camunda-7` | `Camunda7Backend`. Compiles to BPMN and external tasks, driven over REST. |
| `loadshift-camunda-8` | `Camunda8Backend`. Camunda 8 REST API with `zeebe:` BPMN extensions. |
| `loadshift-demo` | Runnable example on `LocalBackend`. |

There are no vendor clients. The Camunda backends are built on ktor, kotlinx-serialization, and
kotlinx-datetime. `camunda-bpmn-model` is a model library, not a client.

## Camunda mapping

Each fan-out level compiles to its own process definition, and each item is its own process instance.
`forEach` becomes an expand service task followed by a multi-instance call activity into the child
level. `ifThen` and `whileLoop` compile to a decision external task, which runs the Kotlin lambda and
writes a boolean variable, plus a BPMN exclusive gateway. `parallel` compiles to parallel gateways.

Camunda 8 jobs and Camunda 7 external tasks differ in retry and timeout semantics. The DSL and the
logic are identical across backends; the engine semantics are not.

## Build and test

```bash
./amper.bat build      # Windows
./amper build          # macOS / Linux
./amper test
```

## Run the demo

```bash
./amper.bat run -m loadshift-demo
```

The Camunda worker loops require a running engine and are covered by opt-in tests. The core, DSL,
compiler, and local interpreter are unit-tested and run in CI.

## License

MIT. See [LICENSE](LICENSE).
