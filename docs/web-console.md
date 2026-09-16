# Web console

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

## Runs of several workers

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
