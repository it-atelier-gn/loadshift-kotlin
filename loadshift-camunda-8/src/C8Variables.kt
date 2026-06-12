package loadshift.camunda8

import kotlinx.datetime.Instant
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
        is JsonElement -> value
        is Map<*, *> -> buildJsonObject {
            for ((k, v) in value) if (k is String) put(k, encode(v))
        }
        is List<*> -> JsonArray(value.map { encode(it) })
        else -> JsonPrimitive(value.toString())
    }

    fun decode(element: JsonElement): Any? = when (element) {
        is JsonNull -> null
        is JsonObject -> element.mapValues { (_, v) -> decode(v) }
        is JsonArray -> element.map { decode(it) }
        is JsonPrimitive ->
            if (element.isString) element.content
            else element.booleanOrNull ?: element.intOrNull ?: element.longOrNull ?: element.doubleOrNull
                ?: element.content
    }
}
