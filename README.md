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
