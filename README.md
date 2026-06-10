# 🔄 loadshift-kotlin

> ⚠️ **Under active development.**

A Kotlin DSL for **batch, cleanup-style durable execution**. You describe a per-item flow once; the
same workflow runs in-process for testing or durably on Camunda 7 / Camunda 8 — no rewrite.

The spine is **hierarchical fan-out**: a large seed list (e.g. millions of customer ids) where each
item expands into children (e.g. contacts), and a unit of work runs per child — arbitrarily nestable.

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

LocalBackend().run(cleanup).await()                          // test instantly, no infra
Camunda7Backend("http://localhost:8080/engine-rest").run(cleanup).await()   // durable
```

## ✨ Features

- **Fan-out DSL** — `forEach(expand) { … }`, nestable to any depth; child type inferred (no factory boilerplate).
- **Control flow** — `ifThen` / `elseThen`, `whileLoop`, `parallel { branch { } }`. Predicates stay
  Kotlin lambdas on every backend (never translated to FEEL/JUEL).
- **Streaming seed** — `items(list)` for small batches, `source { Flow }` / `source(paginate { … })`
  for millions of items without materializing them; bounded by backpressure.
- **Resilience** — per-task `RetryPolicy` (backoff + jitter, classification, timeout),
  `ErrorPolicy` = DeadLetter | Fail | Skip with an exportable dead-letter set.
- **Throughput controls** — `maxConcurrency`, `rateLimit`, per-level `concurrency`.
- **Start modes** — `Start.Now` (autostart), `Start.Manual` (fire via `RunHandle.start()`),
  `Start.At(instant)`, `Start.Cron(expr)`.
- **Dry run** — `LocalBackend().dryRun(wf)` returns the executed topics without side effects.

## 📦 Modules

| Module | Description |
| --- | --- |
| `loadshift-core` | Flow IR, the `@DslMarker` DSL, `Backend` SPI, and the BPMN compiler. |
| `loadshift-local` | In-process interpreter (`LocalBackend`) — for tests/dev, no infrastructure. |
| `loadshift-camunda-7` | `Camunda7Backend` — compiles to BPMN + external tasks, driven over REST (ktor). |
| `loadshift-camunda-8` | `Camunda8Backend` — Camunda 8 (8.8) REST API + `zeebe:` BPMN extensions. |
| `loadshift-demo` | Runnable example on `LocalBackend`. |

No vendor clients — the Camunda backends are hand-rolled on `ktor` + `kotlinx-serialization` +
`kotlinx-datetime`. (`camunda-bpmn-model` is a model library, not a client.)

## 🧩 How it maps to Camunda

Each fan-out level compiles to its **own process definition**; each item is its **own process
instance**. `forEach` becomes an expand service task plus a multi-instance call activity into the
child level. `ifThen` / `whileLoop` compile to a decision external task (it runs your Kotlin lambda
and writes a boolean variable) plus a BPMN exclusive gateway; `parallel` to parallel gateways.

> Camunda 8 jobs and Camunda 7 external tasks differ in retry/timeout semantics; the DSL and your
> logic are identical across backends, the engine semantics are not.

## 🔨 Build & test

```bash
./amper.bat build      # Windows
./amper build          # macOS/Linux
./amper test
```

## ▶️ Run the demo

```bash
./amper.bat run -m loadshift-demo
```

The Camunda backends' worker loops require a running engine and are validated by opt-in tests; the
core, DSL, compiler, and local interpreter are fully unit-tested and run in CI.

## 📄 License

Apache 2.0 — see [LICENSE](LICENSE).
