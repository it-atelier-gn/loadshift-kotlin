package loadshift.camunda7

import kotlinx.datetime.Instant
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull

@Serializable
data class CamundaValue(val value: JsonElement, val type: String? = null)

object CamundaVariables {
    fun toCamunda(map: Map<String, Any?>): Map<String, CamundaValue> =
        map.mapValues { (_, v) -> encode(v) }

    fun toCamunda(json: JsonObject): Map<String, CamundaValue> =
        json.mapValues { (_, v) -> encode(fromJsonElement(v)) }

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
        is Map<*, *>, is List<*> -> CamundaValue(JsonPrimitive(toJsonElement(value).toString()), "json")
        is JsonElement -> CamundaValue(JsonPrimitive(value.toString()), "json")
        else -> CamundaValue(JsonPrimitive(value.toString()), "String")
    }

    fun decode(cv: CamundaValue): Any? {
        val element = cv.value
        if (element is JsonNull) return null
        val primitive = element as? JsonPrimitive ?: return fromJsonElement(element)
        return when (cv.type?.lowercase()) {
            "boolean" -> primitive.booleanOrNull ?: primitive.content.toBoolean()
            "integer" -> primitive.intOrNull ?: primitive.content.toIntOrNull()
            "long" -> primitive.longOrNull ?: primitive.content.toLongOrNull()
            "double" -> primitive.doubleOrNull ?: primitive.content.toDoubleOrNull()
            "json" -> fromJsonElement(Json.parseToJsonElement(primitive.content))
            else -> primitive.content
        }
    }

    fun toJsonElement(value: Any?): JsonElement = when (value) {
        null -> JsonNull
        is Boolean -> JsonPrimitive(value)
        is Int -> JsonPrimitive(value)
        is Long -> JsonPrimitive(value)
        is Double -> JsonPrimitive(value)
        is Float -> JsonPrimitive(value.toDouble())
        is Instant -> JsonPrimitive(value.toString())
        is String -> JsonPrimitive(value)
        is JsonElement -> value
        is Map<*, *> -> buildJsonObject {
            for ((k, v) in value) if (k is String) put(k, toJsonElement(v))
        }
        is List<*> -> JsonArray(value.map { toJsonElement(it) })
        else -> JsonPrimitive(value.toString())
    }

    fun fromJsonElement(element: JsonElement): Any? = when (element) {
        is JsonNull -> null
        is JsonPrimitive ->
            if (element.isString) element.content
            else element.booleanOrNull ?: element.intOrNull ?: element.longOrNull ?: element.doubleOrNull
                ?: element.content
        is JsonObject -> element.mapValues { (_, v) -> fromJsonElement(v) }
        is JsonArray -> element.map { fromJsonElement(it) }
    }
}
