package loadshift.bench

import kotlinx.coroutines.runBlocking
import loadshift.camunda7.Camunda7Backend
import loadshift.camunda8.Camunda8Backend
import loadshift.core.Backend
import loadshift.core.RunConfig
import loadshift.local.LocalBackend
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val options = try {
        BenchOptions.parse(args.toList())
    } catch (e: IllegalArgumentException) {
        System.err.println(e.message)
        System.err.println("options: --backend local|camunda7|camunda8 --base <url> --items <n> --tasks <n> --concurrency <n> --work <ms>")
        exitProcess(2)
    }
    val backend: Backend = when (options.backend) {
        "camunda7" -> Camunda7Backend(options.base ?: "http://localhost:8080/engine-rest")
        "camunda8" -> Camunda8Backend(options.base ?: "http://localhost:8080")
        else -> LocalBackend()
    }
    val report = runBlocking {
        runBenchmark(backend, options.backend, options.items, options.tasks, options.work, RunConfig(maxConcurrency = options.concurrency))
    }
    println(report.render())
    exitProcess(if (report.done == options.items.toLong()) 0 else 1)
}
