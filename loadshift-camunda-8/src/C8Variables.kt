package loadshift.camunda8

import kotlinx.datetime.Instant
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull

object C8Variables {
    fun toJson(map: Map<String, Any?>): JsonObject = buildJsonObject {
        for ((key, value) in map) put(key, encode(value))
    }

    fun fromJson(obj: JsonObject): MutableMap<String, Any?> =
        obj.mapValues { (_, element) -> decode(element) }.toMutableMap()

    fun encode(value: Any?): JsonElement = when (value) {
        null -> JsonNull
        is Boolean -> JsonPrimitive(value)
        is Int -> JsonPrimitive(value)
        is Long -> JsonPrimitive(value)
        is Double -> JsonPrimitive(value)
        is Float -> JsonPrimitive(value.toDouble())
        is Instant -> JsonPrimitive(value.toString())
        is String -> JsonPrimitive(value)
        else -> JsonPrimitive(value.toString())
    }

    fun decode(element: JsonElement): Any? {
        if (element is JsonNull) return null
        val primitive = element as? JsonPrimitive ?: return element.toString()
        if (primitive.isString) return primitive.content
        primitive.booleanOrNull?.let { return it }
        primitive.intOrNull?.let { return it }
        primitive.longOrNull?.let { return it }
        primitive.doubleOrNull?.let { return it }
        return primitive.content
    }
}
