package loadshift.core

import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.serializer

interface WorkItem {
    val key: String? get() = null
}

class WorkItemCodec<W : WorkItem>(
    val decode: (JsonObject) -> W,
    val encode: (W) -> JsonObject,
    val fields: List<String>? = null,
)

@PublishedApi
internal fun fieldsOf(descriptor: SerialDescriptor): List<String>? =
    if (descriptor.kind == StructureKind.CLASS) List(descriptor.elementsCount, descriptor::getElementName) else null

@PublishedApi
internal val codecJson = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
}

inline fun <reified W : WorkItem> workItemCodec(): WorkItemCodec<W> = WorkItemCodec(
    decode = { json -> codecJson.decodeFromJsonElement(json) },
    encode = { item -> codecJson.encodeToJsonElement(item) as JsonObject },
    fields = try {
        fieldsOf(serializer<W>().descriptor)
    } catch (e: SerializationException) {
        null
    },
)
