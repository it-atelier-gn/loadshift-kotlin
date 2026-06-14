package loadshift.engine

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import kotlin.system.exitProcess

fun commandAvailable(cmd: String): Boolean = try {
    val process = ProcessBuilder(cmd, "--version").redirectErrorStream(true).start()
    process.inputStream.readBytes()
    process.waitFor() == 0
} catch (e: Exception) {
    false
}

val cli: String by lazy { if (commandAvailable("podman")) "podman" else "docker" }

fun container(vararg cmd: String): Pair<Int, String> {
    val process = ProcessBuilder(listOf(cli) + cmd).redirectErrorStream(true).start()
    val output = process.inputStream.readBytes().decodeToString()
    return process.waitFor() to output
}

fun containerInherit(vararg cmd: String): Int =
    ProcessBuilder(listOf(cli) + cmd).inheritIO().start().waitFor()

fun main(args: Array<String>) {
    val engine = args.getOrNull(0)
    val action = args.getOrNull(1) ?: "start"
    val port = args.getOrNull(2)?.toIntOrNull() ?: 8080

    if (engine !in setOf("c7", "c8", "cib7") || action !in setOf("start", "stop", "logs")) {
        System.err.println("usage: kotlin run -m engine c7|c8|cib7 [start|stop|logs] [port]")
        exitProcess(1)
    }

    val name = when (engine) {
        "c7" -> "loadshift-dev-camunda7"
        "cib7" -> "loadshift-dev-cibseven"
        else -> "loadshift-dev-camunda8"
    }

    when (action) {
        "stop" -> {
            container("rm", "-f", name)
            println("$name stopped")
            exitProcess(0)
        }
        "logs" -> exitProcess(containerInherit("logs", "-f", name))
    }

    container("rm", "-f", name)

    val (runCode, runOutput) = if (engine == "c7") {
        container("run", "-d", "--name", name, "-p", "$port:8080", "camunda/camunda-bpm-platform:run-7.24.0")
    } else if (engine == "cib7") {
        container("run", "-d", "--name", name, "-p", "$port:8080", "cibseven/cibseven:run-latest")
    } else {
        val config = Paths.get("scripts/engine/c8-application.yaml").toAbsolutePath()
        if (!Files.exists(config)) {
            System.err.println("scripts/engine/c8-application.yaml not found - run from the repository root")
            exitProcess(1)
        }
        container(
            "run", "-d", "--name", name,
            "-p", "$port:8080", "-p", "26500:26500",
            "-v", "$config:/usr/local/camunda/config/application.yaml:ro",
            "camunda/camunda:8.9.8",
        )
    }

    if (runCode != 0) {
        System.err.println(runOutput.trim())
        exitProcess(runCode)
    }

    val probe = if (engine == "c8") "http://localhost:$port/v2/topology" else "http://localhost:$port/engine-rest/version"
    val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()

    println("waiting for $name ...")
    var ready = false
    repeat(120) {
        if (!ready) {
            try {
                val request = HttpRequest.newBuilder(URI(probe)).timeout(Duration.ofSeconds(2)).GET().build()
                if (http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() in 200..299) {
                    ready = true
                }
            } catch (e: Exception) {
            }
            if (!ready) Thread.sleep(2000)
        }
    }
    if (!ready) {
        System.err.println("engine did not become ready; check: $cli logs $name")
        exitProcess(1)
    }

    println()
    if (engine == "c7") {
        println("Camunda 7 ready.")
        println("  REST     http://localhost:$port/engine-rest")
        println("  Cockpit  http://localhost:$port/camunda  (demo/demo)")
        println()
        println("  val backend = Camunda7Backend(\"http://localhost:$port/engine-rest\")")
    } else if (engine == "cib7") {
        println("CIB seven ready.")
        println("  REST     http://localhost:$port/engine-rest")
        println("  Cockpit  http://localhost:$port/camunda/app/  (demo/demo)")
        println()
        println("  val backend = Camunda7Backend(\"http://localhost:$port/engine-rest\")")
    } else {
        println("Camunda 8 ready.")
        println("  REST      http://localhost:$port")
        println("  Operate   http://localhost:$port/operate  (demo/demo)")
        println()
        println("  val backend = Camunda8Backend(\"http://localhost:$port\")")
    }
    println()
    println("stop with: kotlin run -m engine $engine stop")
}
