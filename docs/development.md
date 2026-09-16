# Development

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

The [Dependencies](../.github/workflows/dependencies.yml) workflow runs the check every Monday.

## Publishing

To try local changes in another project, publish the library modules to the local Maven repository. They publish under the group `io.github.it-atelier-gn`. The group and version are set in [templates/library.module-template.yaml](../templates/library.module-template.yaml).

```sh
./kotlin publish mavenLocal \
  -m loadshift-core -m loadshift-local \
  -m loadshift-camunda-7 -m loadshift-camunda-8 -m loadshift-camunda-8-stream \
  -m loadshift-web -m loadshift-sqlite -m loadshift-otel -m loadshift-micrometer
```

## Docs site

[index.html](index.html), [camunda-7-to-8.html](camunda-7-to-8.html) and [examples.html](examples.html) in this folder make up the [docs site](https://it-atelier-gn.github.io/loadshift-kotlin/). [pages.yml](../.github/workflows/pages.yml) deploys the folder to GitHub Pages on every push to `main` that touches `docs/`.
