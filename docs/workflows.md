# Workflows

## Steps

| Step | In-process | On an engine |
| --- | --- | --- |
| `task(topic) { }` | Runs the body with retry, timeout and rate limit | Service task with the job type `<workflow-key>/<topic>` |
| `condition(predicate) { } otherwise { }` | Runs the matching branch | Service task and exclusive gateways |
| `loop(predicate) { }` | Repeats the body while the predicate holds | Service task and a gateway loop |
| `parallel { branch { } }` | Runs the branches concurrently | Parallel gateways |
| `fanOut(expand) { }` | Runs the body per child, `concurrency` at a time | Child process called by a parallel multi-instance call activity |
| `.reduce(initial, combine) { }`, `.collect { }` | Folds the expanded children into the parent | Service task after the call activity |
| `wait(duration)` | Suspends the item | Timer catch event |
| `timeout(duration) { }` | Cancels the block and dead-letters the item | Embedded subprocess with an interrupting boundary timer and a service task that records the dead letter |
| `awaitMessage(name)` | Waits for `send` or `broadcast` | Message catch event |
| `awaitSignal(name)` | Waits for `backend.signal(name)` | Signal catch event |
| `userTask(name, assignee, candidateGroups) { item, form -> }` | Waits until `completeUserTask` is called, then runs the block with the form | User task with its assignment, followed by a service task that runs the block |
| `call(workflow)` | Runs the steps of the other workflow on the item | Service task that hands over the item, call activity of the other workflow's process, service task that writes the item back |
| `task(...) { } compensate { }` | Runs when the item is dead-lettered, in reverse order | Same, from a snapshot stored as a process variable |

`backend.signal(name)` releases every item that waits for the signal at that moment, in every workflow: in-process in all active runs of the `LocalBackend`, on an engine in all process instances of the engine (of the backend's tenant when `tenantId` is set). An item that reaches `awaitSignal` afterwards waits for the next signal.

`backend.userTasks(workflow)` returns the open user tasks of the workflow and of the workflows it calls, each with its id, name, item key, assignee and candidate groups. `backend.completeUserTask(id, form)` completes one with a JSON object and returns `false` for a task that is not open; the block of the step applies the form to the item. On an engine these are the engine's user tasks (Camunda user tasks on Camunda 8), so any task list of the engine can complete them as well when it sets the variable `<step id>_form`.

`call(workflow)` takes a workflow with the same item type; its `input` is not used. Changes the called steps make to the item are visible to the caller afterwards. A dead letter or skip inside the called steps ends them, is recorded under the called workflow's key and level, and the caller continues with the next step. On an engine the run deploys the called workflow together with the caller and works its jobs, and `send` and `broadcast` reach items waiting inside called workflows.

A workflow name becomes its key: lowercased, with every character other than letters, digits, `_` and `-` replaced by `-`. The key must start with a letter or `_`. Topics must be unique within a workflow, and the names `decision_c<n>`, `decision_l<n>`, `expand_f<n>`, `reduce_f<n>`, `timeout_to<n>`, `loop_l<n>`, `call_cw<n>`, `return_cw<n>` and `form_ut<n>` are reserved.
