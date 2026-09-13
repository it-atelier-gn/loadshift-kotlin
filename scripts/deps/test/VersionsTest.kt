package loadshift.deps

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class VersionsTest {

    @Test
    fun parsesLibrariesWithResolvedVersions() {
        val catalog = """
            [versions]
            ktor = "3.5.2"
            sqlite-jdbc = "3.53.4.0"

            [libraries]
            ktor-client-core = { module = "io.ktor:ktor-client-core", version.ref = "ktor" }
            sqlite-jdbc = { module = "org.xerial:sqlite-jdbc", version.ref = "sqlite-jdbc" }
        """.trimIndent()

        assertEquals(
            listOf(
                Library("ktor-client-core", "io.ktor", "ktor-client-core", "3.5.2"),
                Library("sqlite-jdbc", "org.xerial", "sqlite-jdbc", "3.53.4.0"),
            ),
            parseCatalog(catalog),
        )
    }

    @Test
    fun rejectsLibrariesReferencingUnknownVersions() {
        val catalog = """
            [libraries]
            broken = { module = "a:b", version.ref = "missing" }
        """.trimIndent()
        assertFailsWith<IllegalArgumentException> { parseCatalog(catalog) }
    }

    @Test
    fun latestStableIgnoresPreReleasesAndComparesNumerically() {
        val versions = listOf("1.9.0", "1.10.0", "1.12.0-RC", "0.8.0-0.6.x-compat", "2.0.0-beta1")
        assertEquals("1.10.0", latestStable(versions))
        assertNull(latestStable(listOf("1.0.0-RC")))
    }

    @Test
    fun comparesVersionsSegmentBySegment() {
        assertTrue(compareVersions("3.53.4.0", "3.46.1.3") > 0)
        assertEquals(0, compareVersions("1.0", "1.0.0"))
        assertTrue(compareVersions("0.9.9", "0.10.0") < 0)
    }

    @Test
    fun detectsOutdatedStableAndPreReleaseVersions() {
        assertTrue(isOutdated("1.9.0", "1.11.0"))
        assertFalse(isOutdated("1.11.0", "1.11.0"))
        assertTrue(isOutdated("1.12.0-RC", "1.12.0"))
        assertFalse(isOutdated("1.12.0-RC", "1.11.0"))
    }

    @Test
    fun readsMavenMetadataWrapperAndReleaseTag() {
        val metadata = "<metadata><versioning><versions><version>1.0.0</version><version> 1.1.0 </version></versions></versioning></metadata>"
        assertEquals(listOf("1.0.0", "1.1.0"), metadataVersions(metadata))
        assertEquals("0.12.1", wrapperVersion("#!/bin/sh\nkotlin_cli_version=0.12.1\n"))
        assertEquals("0.12.1", releaseTag("""{"url":"x","tag_name":"v0.12.1","name":"0.12.1"}"""))
    }
}
