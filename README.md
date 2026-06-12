# loadshift-kotlin

[![CI](https://github.com/it-atelier-gn/loadshift-kotlin/actions/workflows/ci.yml/badge.svg)](https://github.com/it-atelier-gn/loadshift-kotlin/actions)
[![Kotlin](https://img.shields.io/badge/kotlin-2.x-blueviolet?logo=kotlin)](https://kotlinlang.org/)
[![Amper](https://img.shields.io/badge/build-amper-blue)](https://github.com/JetBrains/amper)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Kotlin DSL for durable execution.

Full examples and reference: **[it-atelier-gn.github.io/loadshift-kotlin](https://it-atelier-gn.github.io/loadshift-kotlin/)**

---

## Modules

| Module | Description |
| --- | --- |
| `loadshift-core` | Flow IR, the DSL, the `Backend` SPI, and the BPMN compiler. |
| `loadshift-local` | In-process interpreter (`LocalBackend`) for tests and development. |
| `loadshift-camunda-7` | `Camunda7Backend`. Compiles to BPMN and external tasks, driven over REST. |
| `loadshift-camunda-8` | `Camunda8Backend`. Camunda 8 REST API with `zeebe:` BPMN extensions. |
| `loadshift-web` | `IntrospectionServer`. Embedded web console showing live runs of any backend. |
| `loadshift-demo` | Runnable example on `LocalBackend`. |

---

## Quick Start

### Prerequisites

- JDK 21+ (the bundled [Amper](https://github.com/JetBrains/amper) wrapper handles everything else)

### Build & Run

```sh
git clone https://github.com/it-atelier-gn/loadshift-kotlin.git
cd loadshift-kotlin
./amper build
./amper test

# run the demo workflow
./amper run -m loadshift-demo

# run the demo with the web console at http://127.0.0.1:8571
./amper run -m loadshift-demo -- --ui
```

---

## End-to-end tests

The Camunda modules contain full-loop e2e tests (`Camunda7E2eTest`, `Camunda8E2eTest`) that deploy a compiled workflow to a real engine, work the external tasks/jobs, and assert the run result. They use [Testcontainers](https://testcontainers.com/) to start minimal engines automatically:

- Camunda 7: `camunda/camunda-bpm-platform:run-7.24.0`
- Camunda 8: `camunda/camunda:8.9.8` with H2 secondary storage and the unprotected API (no Elasticsearch needed)

`./amper test` runs them whenever Docker is available; without Docker they skip. To reuse an already-running engine instead, set `LOADSHIFT_C7_BASE` (e.g. `http://localhost:8080/engine-rest`) or `LOADSHIFT_C8_BASE` (e.g. `http://localhost:8080`).

---

## Examples: DSL → BPMN

`scripts/examples.main.kts` compiles a set of example workflows with both Camunda dialects, verifies the BPMN structurally (service task topics, gateways, call activities, FEEL conditions, complete diagram interchange), and generates [docs/examples.html](https://it-atelier-gn.github.io/loadshift-kotlin/examples.html) — DSL source on the left, the rendered BPMN diagram (bpmn-js) on the right.

Requires the [Kotlin 2.4 command-line compiler](https://kotlinlang.org/docs/command-line.html) and a prior `./amper build`:

```sh
./scripts/examples.sh           # verify + regenerate docs/examples.html
./scripts/examples.sh verify    # structural checks only
# Windows: powershell -File scripts\examples.ps1 [verify]
```

---

## Web Console

Every backend implements the introspection API (`IntrospectableBackend` in `loadshift-core`): run state, progress counters, dead letters, and the flow structure. `loadshift-web` serves it as JSON plus an HTML dashboard:

```kotlin
val backend = LocalBackend()
IntrospectionServer(backend, port = 8571).start()
backend.run(workflow).await()
```

Endpoints: `/` (dashboard), `/api/backend`, `/api/runs`, `/api/runs/{id}`. The Camunda backends additionally report the engine's live instance count per run.

---

## Documentation

The docs site lives in [docs/](docs/) and is deployed to GitHub Pages by [pages.yml](.github/workflows/pages.yml) on every push to `main` that touches `docs/`.

---

## Contributing

Contributions are welcome. For substantial changes, open an issue first to discuss the approach.

---

## License

MIT © 2026 Georg Nelles
