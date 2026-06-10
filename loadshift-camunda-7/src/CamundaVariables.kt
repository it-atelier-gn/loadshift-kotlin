package loadshift.camunda7

import kotlinx.datetime.Instant
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull

@Serializable
data class CamundaValue(val value: JsonElement, val type: String? = null)

object CamundaVariables {
    fun toCamunda(map: Map<String, Any?>): Map<String, CamundaValue> =
        map.mapValues { (_, v) -> encode(v) }

    fun fromCamunda(vars: Map<String, CamundaValue>): MutableMap<String, Any?> =
        vars.mapValues { (_, cv) -> decode(cv) }.toMutableMap()

    fun encode(value: Any?): CamundaValue = when (value) {
        null -> CamundaValue(JsonNull, "Null")
        is Boolean -> CamundaValue(JsonPrimitive(value), "Boolean")
        is Int -> CamundaValue(JsonPrimitive(value), "Integer")
        is Long -> CamundaValue(JsonPrimitive(value), "Long")
        is Double -> CamundaValue(JsonPrimitive(value), "Double")
        is Float -> CamundaValue(JsonPrimitive(value.toDouble()), "Double")
        is Instant -> CamundaValue(JsonPrimitive(value.toString()), "String")
        is String -> CamundaValue(JsonPrimitive(value), "String")
        else -> CamundaValue(JsonPrimitive(value.toString()), "String")
    }

    fun decode(cv: CamundaValue): Any? {
        val element = cv.value
        if (element is JsonNull) return null
        val primitive = element as? JsonPrimitive ?: return element.toString()
        return when (cv.type) {
            "Boolean" -> primitive.booleanOrNull ?: primitive.content.toBoolean()
            "Integer" -> primitive.intOrNull ?: primitive.content.toIntOrNull()
            "Long" -> primitive.longOrNull ?: primitive.content.toLongOrNull()
            "Double" -> primitive.doubleOrNull ?: primitive.content.toDoubleOrNull()
            else -> primitive.content
        }
    }
}
