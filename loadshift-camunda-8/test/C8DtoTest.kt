package loadshift.camunda8

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class C8DtoTest {

    private val json = Json { explicitNulls = false }

    @Test
    fun searchFilterCarriesActiveState() {
        val body = json.encodeToString(SearchRequest(SearchFilter("proc", state = "ACTIVE")))
        assertTrue(body.contains("\"state\":\"ACTIVE\""))
    }

    @Test
    fun searchFilterOmitsNullState() {
        val body = json.encodeToString(SearchRequest(SearchFilter("proc")))
        assertEquals("""{"filter":{"processDefinitionId":"proc"}}""", body)
    }
}
