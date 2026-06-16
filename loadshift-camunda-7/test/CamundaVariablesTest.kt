package loadshift.camunda7

import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CamundaVariablesTest {
    @Test
    fun roundTripsPrimitiveTypes() {
        val map = mapOf(
            "s" to "hello",
            "i" to 5,
            "l" to 9_000_000_000L,
            "d" to 1.5,
            "b" to true,
            "n" to null,
        )
        val back = CamundaVariables.fromCamunda(CamundaVariables.toCamunda(map))
        assertEquals("hello", back["s"])
        assertEquals(5, back["i"])
        assertEquals(9_000_000_000L, back["l"])
        assertEquals(1.5, back["d"])
        assertEquals(true, back["b"])
        assertNull(back["n"])
    }

    @Test
    fun encodesCamundaTypeNames() {
        assertEquals("String", CamundaVariables.encode("x").type)
        assertEquals("Integer", CamundaVariables.encode(1).type)
        assertEquals("Long", CamundaVariables.encode(1L).type)
        assertEquals("Double", CamundaVariables.encode(1.0).type)
        assertEquals("Boolean", CamundaVariables.encode(true).type)
        assertEquals("Null", CamundaVariables.encode(null).type)
    }

    @Test
    fun toCamundaFromJsonObjectProducesTypedValues() {
        val json = buildJsonObject {
            put("name", JsonPrimitive("alice"))
            put("count", JsonPrimitive(7))
            put("flag", JsonPrimitive(true))
        }
        val result = CamundaVariables.toCamunda(json)
        assertEquals("String", result["name"]!!.type)
        assertEquals("alice", (result["name"]!!.value as JsonPrimitive).content)
        assertEquals("Boolean", result["flag"]!!.type)
    }
}
