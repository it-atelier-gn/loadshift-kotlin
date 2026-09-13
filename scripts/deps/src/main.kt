package loadshift.deps

import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlin.system.exitProcess

private class Row(val name: String, val current: String, val latest: String?) {
    val status: String = when {
        latest == null -> "lookup failed"
        isOutdated(current, latest) -> "update available"
        else -> "up to date"
    }
}

private val http: HttpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()

private fun fetch(url: String): String? {
    val request = HttpRequest.newBuilder(URI(url))
        .timeout(Duration.ofSeconds(30))
        .header("User-Agent", "loadshift-deps")
        .GET()
        .build()
    return try {
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() == 200) response.body() else null
    } catch (e: Exception) {
        null
    }
}

private fun mavenRow(library: Library): Row {
    val path = "${library.group.replace('.', '/')}/${library.artifact}"
    val latest = fetch("https://repo1.maven.org/maven2/$path/maven-metadata.xml")?.let { latestStable(metadataVersions(it)) }
    return Row("${library.group}:${library.artifact}", library.version, latest)
}

private fun toolchainRow(wrapper: File): Row? {
    val current = wrapperVersion(wrapper.readText()) ?: return null
    val latest = fetch("https://api.github.com/repos/JetBrains/kotlin-toolchain/releases/latest")?.let(::releaseTag)
    return Row("Kotlin Toolchain", current, latest)
}

fun main(args: Array<String>) {
    val strict = "check" in args
    val catalog = File("libs.versions.toml")
    if (!catalog.exists()) {
        System.err.println("libs.versions.toml not found - run from the repository root")
        exitProcess(2)
    }

    val rows = parseCatalog(catalog.readText())
        .distinctBy { "${it.group}:${it.artifact}:${it.version}" }
        .map(::mavenRow) + listOfNotNull(toolchainRow(File("kotlin")))

    val nameWidth = rows.maxOf { it.name.length }
    val currentWidth = maxOf("current".length, rows.maxOf { it.current.length })
    val latestWidth = maxOf("latest".length, rows.maxOf { (it.latest ?: "-").length })
    println("${"dependency".padEnd(nameWidth)}  ${"current".padEnd(currentWidth)}  ${"latest".padEnd(latestWidth)}  status")
    for (row in rows.sortedBy { it.name }) {
        println("${row.name.padEnd(nameWidth)}  ${row.current.padEnd(currentWidth)}  ${(row.latest ?: "-").padEnd(latestWidth)}  ${row.status}")
    }

    val failed = rows.count { it.latest == null }
    val outdated = rows.count { it.status == "update available" }
    println()
    println("$outdated update(s) available, $failed lookup(s) failed")
    if (strict && failed > 0) exitProcess(2)
    if (strict && outdated > 0) exitProcess(1)
}
