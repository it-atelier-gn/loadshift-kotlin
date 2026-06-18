package loadshift.core

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement

interface WorkItem {
    val key: String? get() = null
}

class WorkItemCodec<W : WorkItem>(
    val decode: (JsonObject) -> W,
    val encode: (W) -> JsonObject,
)

@PublishedApi
internal val codecJson = Json { ignoreUnknownKeys = true }

inline fun <reified W : WorkItem> workItemCodec(): WorkItemCodec<W> = WorkItemCodec(
    decode = { json -> codecJson.decodeFromJsonElement(json) },
    encode = { item -> codecJson.encodeToJsonElement(item) as JsonObject },
)
