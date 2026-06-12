import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import kotlin.system.exitProcess

val engine = args.getOrNull(0)
val action = args.getOrNull(1) ?: "start"
val port = args.getOrNull(2)?.toIntOrNull() ?: 8080

if (engine !in setOf("c7", "c8") || action !in setOf("start", "stop", "logs")) {
    System.err.println("usage: kotlin scripts/engine.main.kts c7|c8 [start|stop|logs] [port]")
    exitProcess(1)
}

val name = if (engine == "c7") "loadshift-dev-camunda7" else "loadshift-dev-camunda8"

fun docker(vararg cmd: String): Pair<Int, String> {
    val process = ProcessBuilder(listOf("docker") + cmd).redirectErrorStream(true).start()
    val output = process.inputStream.readBytes().decodeToString()
    return process.waitFor() to output
}

fun dockerInherit(vararg cmd: String): Int =
    ProcessBuilder(listOf("docker") + cmd).inheritIO().start().waitFor()

when (action) {
    "stop" -> {
        docker("rm", "-f", name)
        println("$name stopped")
        exitProcess(0)
    }
    "logs" -> exitProcess(dockerInherit("logs", "-f", name))
}

docker("rm", "-f", name)

val (runCode, runOutput) = if (engine == "c7") {
    docker("run", "-d", "--name", name, "-p", "$port:8080", "camunda/camunda-bpm-platform:run-7.24.0")
} else {
    val config = Paths.get("scripts/c8-application.yaml").toAbsolutePath()
    if (!Files.exists(config)) {
        System.err.println("scripts/c8-application.yaml not found - run from the repository root")
        exitProcess(1)
    }
    docker(
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

val probe = if (engine == "c7") "http://localhost:$port/engine-rest/version" else "http://localhost:$port/v2/topology"
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
    System.err.println("engine did not become ready; check: docker logs $name")
    exitProcess(1)
}

println()
if (engine == "c7") {
    println("Camunda 7 ready.")
    println("  REST     http://localhost:$port/engine-rest")
    println("  Cockpit  http://localhost:$port/camunda  (demo/demo)")
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
println("stop with: kotlin scripts/engine.main.kts $engine stop")
