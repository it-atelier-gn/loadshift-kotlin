package loadshift.deps

data class Library(val alias: String, val group: String, val artifact: String, val version: String)

private val SECTION = Regex("""^\s*\[([^\]]+)]\s*$""")
private val VERSION_ENTRY = Regex("""^\s*([A-Za-z0-9_.-]+)\s*=\s*"([^"]+)"\s*$""")
private val LIBRARY_ENTRY =
    Regex("""^\s*([A-Za-z0-9_.-]+)\s*=\s*\{\s*module\s*=\s*"([^":]+):([^"]+)"\s*,\s*version\.ref\s*=\s*"([^"]+)"\s*}\s*$""")
private val STABLE = Regex("""^\d+(\.\d+)*$""")
private val NUMERIC_PREFIX = Regex("""^\d+(\.\d+)*""")
private val METADATA_VERSION = Regex("""<version>\s*([^<\s]+)\s*</version>""")
private val WRAPPER_VERSION = Regex("""(?m)^kotlin_cli_version=([A-Za-z0-9._+-]+)\s*$""")
private val RELEASE_TAG = Regex(""""tag_name"\s*:\s*"v?([^"]+)"""")

fun parseCatalog(text: String): List<Library> {
    val versions = mutableMapOf<String, String>()
    val references = mutableListOf<List<String>>()
    var section = ""
    for (line in text.lines()) {
        val header = SECTION.matchEntire(line)
        if (header != null) {
            section = header.groupValues[1].trim()
            continue
        }
        when (section) {
            "versions" -> VERSION_ENTRY.matchEntire(line)?.let { versions[it.groupValues[1]] = it.groupValues[2] }
            "libraries" -> LIBRARY_ENTRY.matchEntire(line)?.let { references += it.groupValues.drop(1) }
        }
    }
    return references.map { (alias, group, artifact, ref) ->
        val version = requireNotNull(versions[ref]) { "library '$alias' references unknown version '$ref'" }
        Library(alias, group, artifact, version)
    }
}

fun compareVersions(a: String, b: String): Int {
    val left = a.split('.').map { it.toInt() }
    val right = b.split('.').map { it.toInt() }
    for (index in 0 until maxOf(left.size, right.size)) {
        val difference = left.getOrElse(index) { 0 }.compareTo(right.getOrElse(index) { 0 })
        if (difference != 0) return difference
    }
    return 0
}

fun isStable(version: String): Boolean = STABLE.matches(version)

fun latestStable(versions: Collection<String>): String? =
    versions.filter(::isStable).maxWithOrNull(::compareVersions)

fun isOutdated(current: String, latestStable: String): Boolean {
    if (isStable(current)) return compareVersions(latestStable, current) > 0
    val base = NUMERIC_PREFIX.find(current)?.value ?: return true
    return compareVersions(latestStable, base) >= 0
}

fun metadataVersions(xml: String): List<String> =
    METADATA_VERSION.findAll(xml).map { it.groupValues[1] }.toList()

fun wrapperVersion(script: String): String? = WRAPPER_VERSION.find(script)?.groupValues?.get(1)

fun releaseTag(json: String): String? = RELEASE_TAG.find(json)?.groupValues?.get(1)
