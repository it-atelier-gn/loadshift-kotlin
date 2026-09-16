package loadshift.examples

import kotlinx.serialization.Serializable
import loadshift.camunda7.Camunda7Dialect
import loadshift.camunda8.Camunda8Dialect
import loadshift.core.BpmnCompiler
import loadshift.core.CompiledProcess
import loadshift.core.EngineNames
import loadshift.core.WorkItem
import loadshift.core.Workflow
import loadshift.core.fanOut
import loadshift.core.task
import loadshift.core.workflow
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes
import org.camunda.bpm.model.bpmn.Bpmn
import org.camunda.bpm.model.bpmn.instance.CallActivity
import org.camunda.bpm.model.bpmn.instance.ConditionExpression
import org.camunda.bpm.model.bpmn.instance.EndEvent
import org.camunda.bpm.model.bpmn.instance.ErrorEventDefinition
import org.camunda.bpm.model.bpmn.instance.FlowNode
import org.camunda.bpm.model.bpmn.instance.MultiInstanceLoopCharacteristics
import org.camunda.bpm.model.bpmn.instance.Process
import org.camunda.bpm.model.bpmn.instance.SequenceFlow
import org.camunda.bpm.model.bpmn.instance.ServiceTask
import org.camunda.bpm.model.bpmn.instance.StartEvent
import org.camunda.bpm.model.bpmn.instance.SubProcess
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnEdge
import org.camunda.bpm.model.bpmn.instance.bpmndi.BpmnShape
import java.io.File

@Serializable
class Order(var id: String, var total: Int = 0, var attempts: Int = 0) : WorkItem {
    override val key get() = id
}

@Serializable
class Line(var sku: String, var qty: Int = 0) : WorkItem {
    override val key get() = sku
}

class OutOfStock : RuntimeException("out of stock")

class Example(
    val id: String,
    val title: String,
    val blurb: String,
    val dsl: String,
    val flow: Workflow<*>,
)

val examples = listOf(
    Example(
        id = "pipeline",
        title = "Sequential pipeline",
        blurb = "Tasks run one after another for every seeded item. Each task becomes an external service task.",
        dsl = """
            workflow<Order>("billing-pipeline") {
                input(orders)
                task("validate") { it.attempts = 0 }
                task("charge") { charge(it) }
                task("receipt") { sendReceipt(it) }
            }
        """.trimIndent(),
        flow = workflow<Order>("billing-pipeline") {
            input(emptyList())
            task("validate") { }
            task("charge") { }
            task("receipt") { }
        },
    ),
    Example(
        id = "branching",
        title = "Conditional branch",
        blurb = "condition/otherwise compiles to a decision service task plus an exclusive gateway pair.",
        dsl = """
            workflow<Order>("order-triage") {
                input(orders)
                condition({ it.total > 100 }) {
                    task("manual-review") { review(it) }
                } otherwise {
                    task("auto-approve") { approve(it) }
                }
            }
        """.trimIndent(),
        flow = workflow<Order>("order-triage") {
            input(emptyList())
            condition({ it.total > 100 }) {
                task("manual-review") { }
            } otherwise {
                task("auto-approve") { }
            }
        },
    ),
    Example(
        id = "loop",
        title = "While loop",
        blurb = "loop compiles to a decision service task with a gateway looping back while the predicate holds.",
        dsl = """
            workflow<Order>("retry-charge") {
                input(orders)
                loop({ it.attempts < 3 }) {
                    task("try-charge") { it.attempts = it.attempts + 1 }
                }
            }
        """.trimIndent(),
        flow = workflow<Order>("retry-charge") {
            input(emptyList())
            loop({ it.attempts < 3 }) {
                task("try-charge") { it.attempts = it.attempts + 1 }
            }
        },
    ),
    Example(
        id = "parallel",
        title = "Parallel branches",
        blurb = "parallel branches compile to a parallel gateway fork/join.",
        dsl = """
            workflow<Order>("fulfilment") {
                input(orders)
                parallel {
                    branch { task("reserve-stock") { reserve(it) } }
                    branch { task("notify-customer") { notify(it) } }
                }
                task("dispatch") { dispatch(it) }
            }
        """.trimIndent(),
        flow = workflow<Order>("fulfilment") {
            input(emptyList())
            parallel {
                branch { task("reserve-stock") { } }
                branch { task("notify-customer") { } }
            }
            task("dispatch") { }
        },
    ),
    Example(
        id = "fanout",
        title = "Fan-out to child items",
        blurb = "fanOut expands every item into children. It compiles to an expand service task plus a parallel multi-instance call activity invoking a child process.",
        dsl = """
            workflow<Order>("order-lines") {
                input(orders)
                task("load") { load(it) }
                fanOut(expand = { fetchLines(it.id) }, concurrency = 4) {
                    condition({ it.qty > 10 }) {
                        task("bulk-price") { bulk(it) }
                    } otherwise {
                        task("unit-price") { unit(it) }
                    }
                }
            }
        """.trimIndent(),
        flow = workflow<Order>("order-lines") {
            input(emptyList())
            task("load") { }
            fanOut(expand = { emptyList<Line>() }, concurrency = 4) {
                condition({ it.qty > 10 }) {
                    task("bulk-price") { }
                } otherwise {
                    task("unit-price") { }
                }
            }
        },
    ),
    Example(
        id = "fanin",
        title = "Fan-in to an aggregate",
        blurb = "reduce folds the expanded children into one value and runs onComplete on the parent. It compiles to the fan-out plus a reduce service task after the multi-instance join.",
        dsl = """
            workflow<Order>("order-totals") {
                input(orders)
                fanOut(expand = { fetchLines(it.id) }, concurrency = 4) {
                    task("price") { price(it) }
                }.reduce(0, combine = { sum, line -> sum + line.qty }) { order, units ->
                    order.total = units
                }
            }
        """.trimIndent(),
        flow = workflow<Order>("order-totals") {
            input(emptyList())
            fanOut(expand = { emptyList<Line>() }, concurrency = 4) {
                task("price") { }
            }.reduce(0, combine = { sum, line -> sum + line.qty }) { order, units ->
                order.total = units
            }
        },
    ),
    Example(
        id = "timer",
        title = "Timed wait",
        blurb = "wait pauses the flow for a fixed duration. It compiles to an intermediate timer catch event the engine schedules natively.",
        dsl = """
            workflow<Order>("retry-later") {
                input(orders)
                task("attempt") { charge(it) }
                wait(15.minutes)
                task("settle") { settle(it) }
            }
        """.trimIndent(),
        flow = workflow<Order>("retry-later") {
            input(emptyList())
            task("attempt") { }
            wait(15.minutes)
            task("settle") { }
        },
    ),
    Example(
        id = "await",
        title = "Wait for an event",
        blurb = "awaitMessage parks the flow until an external event arrives. It compiles to an intermediate message catch event the engine correlates by key.",
        dsl = """
            workflow<Order>("await-payment") {
                input(orders)
                task("invoice") { invoice(it) }
                awaitMessage("payment-confirmed")
                task("fulfil") { fulfil(it) }
            }
        """.trimIndent(),
        flow = workflow<Order>("await-payment") {
            input(emptyList())
            task("invoice") { }
            awaitMessage("payment-confirmed")
            task("fulfil") { }
        },
    ),
    Example(
        id = "await-timeout",
        title = "Event with data or a deadline",
        blurb = "awaitMessage with a block applies the data sent along with the message to the item. With a timeout, an event-based gateway races the message against a timer; when the timer wins, the item runs the onTimeout steps. Both paths join before the next task.",
        dsl = """
            workflow<Order>("await-or-remind") {
                input(orders)
                awaitMessage("payment-confirmed", timeout = 3.days) { order, data ->
                    order.total = data.getValue("amount").jsonPrimitive.int
                } onTimeout {
                    task("send-reminder") { remind(it) }
                }
                task("fulfil") { fulfil(it) }
            }
        """.trimIndent(),
        flow = workflow<Order>("await-or-remind") {
            input(emptyList())
            awaitMessage("payment-confirmed", timeout = 3.days) { _, _ -> } onTimeout {
                task("send-reminder") { }
            }
            task("fulfil") { }
        },
    ),
    Example(
        id = "catching",
        title = "Caught business error",
        blurb = "catching attaches an error boundary event to the task. When the task throws the caught exception, the worker ends the job with a BPMN error and the item takes the branch without further attempts, then continues with the next task.",
        dsl = """
            workflow<Order>("reserve-or-backorder") {
                input(orders)
                task("reserve") { reserve(it) }
                    .catching<OutOfStock> { task("backorder") { backorder(it) } }
                task("ship") { ship(it) }
            }
        """.trimIndent(),
        flow = workflow<Order>("reserve-or-backorder") {
            input(emptyList())
            task("reserve") { }.catching<OutOfStock> { task("backorder") { } }
            task("ship") { }
        },
    ),
    Example(
        id = "timeout",
        title = "Time-boxed scope",
        blurb = "timeout compiles to an embedded subprocess with an interrupting boundary timer. When the timer fires, a service task records a dead letter for the item and the item ends.",
        dsl = """
            workflow<Order>("vendor-call") {
                input(orders)
                timeout(10.minutes) {
                    task("call-vendor") { callVendor(it) }
                    task("store-answer") { store(it) }
                }
                task("confirm") { confirm(it) }
            }
        """.trimIndent(),
        flow = workflow<Order>("vendor-call") {
            input(emptyList())
            timeout(10.minutes) {
                task("call-vendor") { }
                task("store-answer") { }
            }
            task("confirm") { }
        },
    ),
)

class Compiled(val dialect: String, val levels: List<CompiledProcess>, val xml: Map<String, String>)

fun compile(flow: Workflow<*>, dialect: String): Compiled {
    val levels = BpmnCompiler.compile(flow)
    for (level in levels) {
        when (dialect) {
            "camunda7" -> Camunda7Dialect.decorate(level.model, level.serviceTasks)
            "camunda8" -> Camunda8Dialect.decorate(level.model, level.serviceTasks)
        }
    }
    return Compiled(dialect, levels, levels.associate { it.key to Bpmn.convertToString(it.model) })
}

class Failure(val example: String, val dialect: String, val process: String, val message: String)

fun verify(example: Example, compiled: Compiled): Pair<Int, List<Failure>> {
    var checks = 0
    val failures = mutableListOf<Failure>()
    val keys = compiled.levels.map { it.key }.toSet()

    fun check(process: String, condition: Boolean, message: String) {
        checks++
        if (!condition) failures += Failure(example.id, compiled.dialect, process, message)
    }

    for (level in compiled.levels) {
        val model = level.model
        val key = level.key

        runCatching { Bpmn.validateModel(model) }
            .onFailure { failures += Failure(example.id, compiled.dialect, key, "model validation: ${it.message}") }
        checks++

        val process = model.getModelElementById<Process>(key)
        check(key, process.getChildElementsByType(StartEvent::class.java).size == 1, "expected exactly one process start event")
        check(key, model.getModelElementsByType(EndEvent::class.java).isNotEmpty(), "expected at least one end event")

        val terminate = model.getModelElementById<SubProcess>(BpmnCompiler.TERMINATE_SCOPE_ID)
        check(key, terminate?.triggeredByEvent() == true, "missing terminate event subprocess")
        val caught = terminate?.getChildElementsByType(StartEvent::class.java)
            ?.flatMap { it.eventDefinitions }
            ?.filterIsInstance<ErrorEventDefinition>()
            ?.mapNotNull { it.error?.errorCode }
        check(key, caught == listOf(EngineNames.TERMINATE_ERROR), "terminate event subprocess must catch '${EngineNames.TERMINATE_ERROR}'")

        for (ref in level.serviceTasks) {
            val task = model.getModelElementById<ServiceTask>(ref.id)
            check(key, task != null, "service task '${ref.id}' missing")
            if (task == null) continue
            when (compiled.dialect) {
                "camunda7" -> {
                    check(key, task.camundaType == "external", "'${ref.id}' not camunda:type=external")
                    check(key, task.camundaTopic == ref.jobType, "'${ref.id}' topic != '${ref.jobType}'")
                }
                "camunda8" -> {
                    val type = task.extensionElements
                        ?.domElement?.childElements
                        ?.firstOrNull { it.localName == "taskDefinition" }
                        ?.getAttribute("type")
                    check(key, type == ref.jobType, "'${ref.id}' zeebe taskDefinition type != '${ref.jobType}'")
                }
            }
        }

        for (call in model.getModelElementsByType(CallActivity::class.java)) {
            check(key, call.calledElement in keys, "callActivity '${call.id}' targets unknown process '${call.calledElement}'")
            val mi = call.loopCharacteristics as? MultiInstanceLoopCharacteristics
            check(key, mi != null, "callActivity '${call.id}' missing multi-instance loop")
            if (mi == null) continue
            when (compiled.dialect) {
                "camunda7" -> {
                    check(key, mi.camundaCollection?.endsWith(".elements()}") == true, "MI collection not iterating Spin elements()")
                    val targets = call.extensionElements?.domElement?.childElements
                        ?.filter { it.localName == "in" }
                        ?.map { it.getAttribute("target") }
                        .orEmpty()
                    check(key, mi.camundaElementVariable in targets, "callActivity '${call.id}' does not map the child item")
                    check(key, EngineNames.ITEM_KEY in targets, "callActivity '${call.id}' does not map '${EngineNames.ITEM_KEY}'")
                    check(key, EngineNames.WORKFLOW in targets, "callActivity '${call.id}' does not map '${EngineNames.WORKFLOW}'")
                }
                "camunda8" -> {
                    val loop = mi.extensionElements?.domElement?.childElements?.firstOrNull { it.localName == "loopCharacteristics" }
                    check(key, loop?.getAttribute("inputCollection")?.startsWith("=") == true, "MI missing zeebe inputCollection FEEL expr")
                    val called = call.extensionElements?.domElement?.childElements?.firstOrNull { it.localName == "calledElement" }
                    check(key, called?.getAttribute("processId") in keys, "zeebe calledElement processId not a compiled process")
                    val inputs = call.extensionElements?.domElement?.childElements
                        ?.filter { it.localName == "ioMapping" }
                        ?.flatMap { mapping -> mapping.childElements.filter { it.localName == "input" } }
                        ?.map { it.getAttribute("target") }
                        .orEmpty()
                    check(key, EngineNames.ITEM_KEY in inputs, "callActivity '${call.id}' does not map '${EngineNames.ITEM_KEY}'")
                }
            }
        }

        if (compiled.dialect == "camunda8") {
            for (condition in model.getModelElementsByType(ConditionExpression::class.java)) {
                check(key, condition.textContent.startsWith("="), "condition '${condition.textContent}' is not a FEEL expression")
            }
            check(key, "\${" !in compiled.xml.getValue(key), "JUEL expression left in zeebe BPMN")
        }

        val flowNodes = model.getModelElementsByType(FlowNode::class.java).size
        val shapes = model.getModelElementsByType(BpmnShape::class.java).size
        val flows = model.getModelElementsByType(SequenceFlow::class.java).size
        val edges = model.getModelElementsByType(BpmnEdge::class.java).size
        check(key, shapes == flowNodes, "DI shapes ($shapes) != flow nodes ($flowNodes), diagram not renderable")
        check(key, edges == flows, "DI edges ($edges) != sequence flows ($flows), diagram not renderable")
    }
    return checks to failures
}

fun htmlEscape(s: String): String = s
    .replace("&", "&amp;")
    .replace("<", "&lt;")
    .replace(">", "&gt;")

fun generate(target: File, verified: Map<String, Int>) {
    val sections = StringBuilder()
    val index = StringBuilder()
    for (example in examples) {
        val c7 = compile(example.flow, "camunda7")
        val c8 = compile(example.flow, "camunda8")
        val tabs = StringBuilder()
        val panes = StringBuilder()
        val xmlBlocks = StringBuilder()
        c7.levels.forEachIndexed { levelIndex, level ->
            val active = levelIndex == 0
            val label = if (levelIndex == 0) "Process" else "Child ${level.key.substringAfterLast('_')}"
            tabs.append(
                """<button class="leveltab${if (active) " active" else ""}" type="button" aria-pressed="$active" data-target="${example.id}-$levelIndex">$label</button>""",
            )
            panes.append(
                """<div class="pane${if (active) " active" else ""}" id="${example.id}-$levelIndex" data-xml="xml-${example.id}-$levelIndex" role="img" aria-label="BPMN diagram of ${level.key}"></div>""",
            )
            xmlBlocks.append("<script type=\"text/xml\" id=\"xml-${example.id}-$levelIndex\">${c7.xml.getValue(level.key)}</script>\n")
        }
        val rawXml = StringBuilder()
        for ((dialect, compiled) in listOf("Camunda 7" to c7, "Camunda 8" to c8)) {
            for (level in compiled.levels) {
                rawXml.append(
                    """<details class="xml-source"><summary>$dialect BPMN of ${level.key}</summary><pre><code>${htmlEscape(compiled.xml.getValue(level.key))}</code></pre></details>
""",
                )
            }
        }
        val levelTabs = if (c7.levels.size > 1) """<div class="leveltabs" role="group" aria-label="Processes of ${example.id}">$tabs</div>""" else ""
        index.append("""<a href="#${example.id}">${example.title}</a>
""")
        sections.append(
            """<section class="block example" id="${example.id}">
<h2>${example.title}</h2>
<p class="example-meta">Compiled for Camunda 7 and Camunda 8 and verified with ${verified.getValue(example.id)} checks.</p>
<p>${example.blurb}</p>
<div class="split">
<div class="codecard">
<div class="codecard-head"><span>Kotlin</span><button class="copy" type="button">Copy</button></div>
<pre><code class="lang-kotlin">${htmlEscape(example.dsl)}</code></pre>
</div>
<div class="diagram-panel">
<div class="diagram-head"><span>BPMN</span>$levelTabs</div>
$panes
</div>
</div>
$rawXml$xmlBlocks</section>
""",
        )
    }

    val js = """
document.querySelectorAll('.leveltabs').forEach(function (group) {
  group.addEventListener('click', function (event) {
    var button = event.target.closest('.leveltab');
    if (!button) return;
    var section = button.closest('.example');
    section.querySelectorAll('.leveltab').forEach(function (t) {
      var on = t === button;
      t.classList.toggle('active', on);
      t.setAttribute('aria-pressed', on ? 'true' : 'false');
    });
    section.querySelectorAll('.pane').forEach(function (p) { p.classList.toggle('active', p.id === button.dataset.target); });
    render(section.querySelector('#' + button.dataset.target));
  });
});

var rendered = {};
function render(pane) {
  if (rendered[pane.id]) return;
  rendered[pane.id] = true;
  var xml = document.getElementById(pane.dataset.xml).textContent;
  var viewer = new BpmnJS({ container: pane });
  viewer.importXML(xml).then(function () {
    viewer.get('canvas').zoom('fit-viewport', 'auto');
  }).catch(function (err) {
    pane.textContent = 'The diagram could not be rendered: ' + err.message;
  });
}

document.querySelectorAll('.pane.active').forEach(render);
"""

    target.writeText(
        """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>loadshift: Kotlin workflows and the Camunda 7 and Camunda 8 BPMN they compile to</title>
<meta name="description" content="Kotlin workflow definitions next to the Camunda 7 and Camunda 8 BPMN they compile to: pipelines, conditions, retries, fan-out, messages with timeouts, caught errors and time-boxed scopes.">
<link rel="canonical" href="https://it-atelier-gn.github.io/loadshift-kotlin/examples.html">
<meta property="og:type" content="website">
<meta property="og:site_name" content="loadshift">
<meta property="og:title" content="Kotlin workflows and the BPMN they compile to">
<meta property="og:description" content="Kotlin workflow definitions next to the BPMN they compile to for Camunda 7 and Camunda 8.">
<meta property="og:url" content="https://it-atelier-gn.github.io/loadshift-kotlin/examples.html">
<meta property="og:image" content="https://it-atelier-gn.github.io/loadshift-kotlin/og.png">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&family=Schibsted+Grotesk:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://unpkg.com/bpmn-js@17.11.1/dist/assets/bpmn-js.css">
<link rel="stylesheet" href="style.css">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 22 22'><rect width='22' height='22' rx='4' fill='%23f3f5f2'/><circle cx='5' cy='11' r='3.2' fill='none' stroke='%2317202b' stroke-width='1.8'/><rect x='10' y='5' width='10' height='12' rx='2.5' fill='none' stroke='%2317202b' stroke-width='1.8'/><path d='M8.2 11H10' stroke='%2317202b' stroke-width='1.8'/></svg>">
</head>
<body>
<a class="skip" href="#main">Skip to content</a>

<header class="topbar">
  <div class="wrap">
    <a class="brand" href="index.html"><svg class="brand-mark" viewBox="0 0 22 22" aria-hidden="true"><circle cx="5" cy="11" r="3.2" fill="none" stroke="currentColor" stroke-width="1.8"/><rect x="10" y="5" width="10" height="12" rx="2.5" fill="none" stroke="currentColor" stroke-width="1.8"/><path d="M8.2 11H10" stroke="currentColor" stroke-width="1.8"/></svg><span>loadshift</span><span class="brand-lang">kotlin</span></a>
    <nav class="topnav" aria-label="Main">
      <a class="nav-guide" href="index.html#guide">Guide</a>
      <a href="examples.html" aria-current="page">BPMN examples</a>
      <a href="camunda-7-to-8.html">Camunda 7 to 8</a>
      <a href="https://github.com/it-atelier-gn/loadshift-kotlin">GitHub</a>
    </nav>
  </div>
</header>

<main id="main">

<section class="hero hero-page">
  <div class="wrap">
    <h1>Kotlin workflows and the BPMN they compile to</h1>
    <p class="hero-sub">Each example shows the workflow code next to the process it becomes. Every example is compiled for Camunda&nbsp;7 and Camunda&nbsp;8 and checked before this page is generated. Every process also contains the <code>on_terminate</code> event subprocess, which ends dead-lettered and skipped items.</p>
  </div>
</section>

<section class="guide">
  <div class="wrap">
    <div class="layout">
      <nav class="sidenav" aria-label="Examples">
$index      </nav>
      <div class="content">
$sections      </div>
    </div>
  </div>
</section>

</main>

<aside class="about" aria-label="About the author">
  <div class="wrap about-inner">
    <p>Built by <a href="http://www.georg-nelles.de" rel="noopener">Georg Nelles</a>, freelance software engineer
    specializing in automation, modernization and pragmatic tooling. loadshift is open source and runs wherever
    your workflows need to.</p>
    <div class="about-links">
      <a class="btn btn-plain" href="http://www.georg-nelles.de" rel="noopener">georg-nelles.de</a>
      <a class="btn btn-plain" href="https://github.com/it-atelier-gn" rel="noopener">More on GitHub</a>
    </div>
  </div>
</aside>

<footer class="footer">
  <div class="wrap">
    <a class="brand" href="index.html"><svg class="brand-mark" viewBox="0 0 22 22" aria-hidden="true"><circle cx="5" cy="11" r="3.2" fill="none" stroke="currentColor" stroke-width="1.8"/><rect x="10" y="5" width="10" height="12" rx="2.5" fill="none" stroke="currentColor" stroke-width="1.8"/><path d="M8.2 11H10" stroke="currentColor" stroke-width="1.8"/></svg><span>loadshift</span></a>
    <p>&copy; 2026 Georg Nelles. MIT License.</p>
    <nav class="footer-links" aria-label="Project">
      <a href="https://github.com/it-atelier-gn/loadshift-kotlin">GitHub</a>
      <a href="https://github.com/it-atelier-gn/loadshift-kotlin/releases">Releases</a>
      <a href="https://github.com/it-atelier-gn/loadshift-kotlin/issues">Issues</a>
    </nav>
  </div>
</footer>

<script src="app.js"></script>
<script src="https://unpkg.com/bpmn-js@17.11.1/dist/bpmn-navigated-viewer.production.min.js"></script>
<script>
$js
</script>
</body>
</html>
""",
    )
}

fun main(args: Array<String>) {
    val mode = args.firstOrNull() ?: "all"
    var failed = false
    val verifiedChecks = mutableMapOf<String, Int>()

    for (example in examples) {
        var total = 0
        for (dialect in listOf("camunda7", "camunda8")) {
            val compiled = compile(example.flow, dialect)
            val (checks, failures) = verify(example, compiled)
            total += checks
            if (failures.isEmpty()) {
                println("[PASS] ${example.id} @ $dialect ($checks checks)")
            } else {
                failed = true
                for (failure in failures) {
                    println("[FAIL] ${failure.example} @ ${failure.dialect} / ${failure.process}: ${failure.message}")
                }
            }
        }
        verifiedChecks[example.id] = total
    }

    if (failed) {
        println("verification FAILED")
        kotlin.system.exitProcess(1)
    }

    if (mode == "all" || mode == "generate") {
        val out = File("docs/examples.html")
        generate(out, verifiedChecks)
        println("wrote ${out.path}")
    }
}
