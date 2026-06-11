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
```

---

## Documentation

The docs site lives in [docs/](docs/) and is deployed to GitHub Pages by [pages.yml](.github/workflows/pages.yml) on every push to `main` that touches `docs/`.

---

## Contributing

Contributions are welcome. For substantial changes, open an issue first to discuss the approach.

---

## License

MIT © 2026 Georg Nelles
