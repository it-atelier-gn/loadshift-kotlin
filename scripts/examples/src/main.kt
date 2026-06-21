package loadshift.examples

import kotlinx.serialization.Serializable
import loadshift.camunda7.Camunda7Dialect
import loadshift.camunda8.Camunda8Dialect
import loadshift.core.BpmnCompiler
import loadshift.core.CompiledProcess
import loadshift.core.WorkItem
import loadshift.core.Workflow
import loadshift.core.fanOut
import loadshift.core.task
import loadshift.core.workflow
import kotlin.time.Duration.Companion.minutes
import org.camunda.bpm.model.bpmn.Bpmn
import org.camunda.bpm.model.bpmn.instance.CallActivity
import org.camunda.bpm.model.bpmn.instance.ConditionExpression
import org.camunda.bpm.model.bpmn.instance.EndEvent
import org.camunda.bpm.model.bpmn.instance.FlowNode
import org.camunda.bpm.model.bpmn.instance.MultiInstanceLoopCharacteristics
import org.camunda.bpm.model.bpmn.instance.SequenceFlow
import org.camunda.bpm.model.bpmn.instance.ServiceTask
import org.camunda.bpm.model.bpmn.instance.StartEvent
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

        check(key, model.getModelElementsByType(StartEvent::class.java).size == 1, "expected exactly one start event")
        check(key, model.getModelElementsByType(EndEvent::class.java).isNotEmpty(), "expected at least one end event")

        for (ref in level.serviceTasks) {
            val task = model.getModelElementById<ServiceTask>(ref.id)
            check(key, task != null, "service task '${ref.id}' missing")
            if (task == null) continue
            when (compiled.dialect) {
                "camunda7" -> {
                    check(key, task.camundaType == "external", "'${ref.id}' not camunda:type=external")
                    check(key, task.camundaTopic == ref.topic, "'${ref.id}' topic != '${ref.topic}'")
                }
                "camunda8" -> {
                    val type = task.extensionElements
                        ?.domElement?.childElements
                        ?.firstOrNull { it.localName == "taskDefinition" }
                        ?.getAttribute("type")
                    check(key, type == ref.topic, "'${ref.id}' zeebe taskDefinition type != '${ref.topic}'")
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
                    val hasIn = call.extensionElements?.domElement?.childElements?.any { it.localName == "in" } == true
                    check(key, hasIn, "callActivity '${call.id}' missing camunda:in mapping")
                }
                "camunda8" -> {
                    val loop = mi.extensionElements?.domElement?.childElements?.firstOrNull { it.localName == "loopCharacteristics" }
                    check(key, loop?.getAttribute("inputCollection")?.startsWith("=") == true, "MI missing zeebe inputCollection FEEL expr")
                    val called = call.extensionElements?.domElement?.childElements?.firstOrNull { it.localName == "calledElement" }
                    check(key, called?.getAttribute("processId") in keys, "zeebe calledElement processId not a compiled process")
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
    for (example in examples) {
        val c7 = compile(example.flow, "camunda7")
        val c8 = compile(example.flow, "camunda8")
        val tabs = StringBuilder()
        val panes = StringBuilder()
        val xmlBlocks = StringBuilder()
        c7.levels.forEachIndexed { index, level ->
            val active = if (index == 0) " active" else ""
            tabs.append("""<button class="tab$active" data-target="${example.id}-$index">${level.key}</button>""")
            panes.append("""<div class="pane$active" id="${example.id}-$index" data-xml="xml-${example.id}-$index"></div>""")
            xmlBlocks.append("<script type=\"text/xml\" id=\"xml-${example.id}-$index\">${c7.xml.getValue(level.key)}</script>\n")
        }
        val rawXml = StringBuilder()
        for ((dialect, compiled) in listOf("Camunda 7" to c7, "Camunda 8" to c8)) {
            for (level in compiled.levels) {
                rawXml.append(
                    """<details><summary>$dialect · ${level.key}.bpmn</summary><pre class="xml">${htmlEscape(compiled.xml.getValue(level.key))}</pre></details>""",
                )
            }
        }
        sections.append(
            """
            <section class="example" id="${example.id}">
              <header>
                <h2>${example.title}</h2>
                <span class="badge">verified · ${verified.getValue(example.id)} checks</span>
              </header>
              <p>${example.blurb}</p>
              <div class="split">
                <div class="left">
                  <div class="label">DSL</div>
                  <pre class="dsl"><code>${htmlEscape(example.dsl)}</code></pre>
                </div>
                <div class="right">
                  <div class="label">BPMN <span class="tabs">$tabs</span></div>
                  $panes
                </div>
              </div>
              $rawXml
              $xmlBlocks
            </section>
            """.trimIndent(),
        )
    }

    val js = """
    document.querySelectorAll('.tabs').forEach(function (tabs) {
      tabs.addEventListener('click', function (event) {
        var button = event.target.closest('.tab');
        if (!button) return;
        var section = button.closest('.example');
        section.querySelectorAll('.tab').forEach(function (t) { t.classList.remove('active'); });
        section.querySelectorAll('.pane').forEach(function (p) { p.classList.remove('active'); });
        button.classList.add('active');
        var pane = section.querySelector('#' + button.dataset.target);
        pane.classList.add('active');
        render(pane);
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
        pane.textContent = 'render failed: ' + err.message;
      });
    }

    document.querySelectorAll('.pane.active').forEach(render);
    """.trimIndent()

    val css = """
    :root { --bg:#0b0e0c; --ink:#d8e6d2; --dim:#6d7f6a; --line:#243126; --amber:#ffb454; --green:#7ce38b; }
    * { box-sizing:border-box; margin:0; padding:0; }
    body { background:var(--bg); color:var(--ink); font-family:ui-monospace,'Cascadia Code',Consolas,monospace; padding:2rem; }
    h1 { color:var(--amber); letter-spacing:.25em; text-transform:uppercase; font-size:1.3rem; margin-bottom:.4rem; }
    .sub { color:var(--dim); font-size:.8rem; margin-bottom:2rem; }
    .example { border:1px solid var(--line); margin-bottom:2rem; padding:1.2rem 1.4rem; }
    .example header { display:flex; align-items:baseline; gap:1rem; margin-bottom:.4rem; }
    .example h2 { font-size:1rem; letter-spacing:.05em; }
    .badge { font-size:.65rem; color:var(--green); border:1px solid var(--green); padding:.15em .6em; letter-spacing:.15em; text-transform:uppercase; }
    .example p { color:var(--dim); font-size:.8rem; margin-bottom:1rem; }
    .split { display:grid; grid-template-columns:minmax(320px,5fr) minmax(380px,7fr); gap:1rem; }
    @media (max-width:900px) { .split { grid-template-columns:1fr; } }
    .label { font-size:.65rem; color:var(--dim); letter-spacing:.2em; text-transform:uppercase; margin-bottom:.4rem; display:flex; gap:1rem; align-items:center; }
    .dsl { background:#080a09; border:1px solid var(--line); padding:1rem; overflow-x:auto; font-size:.78rem; line-height:1.5; height:420px; }
    .pane { display:none; background:#f6f8f6; border:1px solid var(--line); height:420px; }
    .pane.active { display:block; }
    .tab { background:none; border:1px solid var(--line); color:var(--dim); font:inherit; font-size:.65rem; padding:.15em .6em; cursor:pointer; }
    .tab.active { color:var(--amber); border-color:var(--amber); }
    details { margin-top:.6rem; font-size:.7rem; color:var(--dim); }
    details summary { cursor:pointer; }
    .xml { max-height:240px; overflow:auto; font-size:.65rem; background:#080a09; border:1px solid var(--line); padding:.8rem; margin-top:.4rem; color:var(--ink); }
    a { color:var(--amber); }
    """.trimIndent()

    target.writeText(
        """
        <!doctype html>
        <html lang="en">
        <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>loadshift · DSL to BPMN examples</title>
        <style>
        $css
        </style>
        </head>
        <body>
        <h1>▞▞ loadshift examples</h1>
        <div class="sub">left: the Kotlin DSL · right: the BPMN it compiles to · generated by scripts/examples · <a href="index.html">back to docs</a></div>
        $sections
        <script src="https://unpkg.com/bpmn-js@17.11.1/dist/bpmn-navigated-viewer.production.min.js"></script>
        <script>
        $js
        </script>
        </body>
        </html>
        """.trimIndent(),
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
