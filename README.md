# loadshift-kotlin

A Kotlin DSL for durable execution. You describe flow and run
in locally or durably on Camunda 7 / Camunda 8.

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

## Modules

| Module | Description |
| --- | --- |
| `loadshift-core` | Flow IR, the DSL, the `Backend` SPI, and the BPMN compiler. |
| `loadshift-local` | In-process interpreter (`LocalBackend`) for tests and development. |
| `loadshift-camunda-7` | `Camunda7Backend`. Compiles to BPMN and external tasks, driven over REST. |
| `loadshift-camunda-8` | `Camunda8Backend`. Camunda 8 REST API with `zeebe:` BPMN extensions. |
| `loadshift-demo` | Runnable example on `LocalBackend`. |

## Build and test

```bash
./amper build
./amper test
```

## Run the demo

```bash
./amper run -m loadshift-demo
```

## License

MIT. See [LICENSE](LICENSE).
