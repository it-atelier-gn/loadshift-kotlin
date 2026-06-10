package loadshift.core

import kotlin.reflect.KProperty

abstract class WorkItemBase(protected val variables: MutableMap<String, Any?> = mutableMapOf()) {
    fun toMap(): Map<String, Any?> = variables.toMap()

    internal fun hydrate(values: Map<String, Any?>) {
        variables.putAll(values)
    }
}

class RequiredVar<T>(private val variables: MutableMap<String, Any?>) {
    @Suppress("UNCHECKED_CAST")
    operator fun getValue(thisRef: Any?, property: KProperty<*>): T =
        (variables[property.name]
            ?: error("Missing required variable '${property.name}'")) as T

    operator fun setValue(thisRef: Any?, property: KProperty<*>, value: T) {
        variables[property.name] = value
    }
}

class OptionalVar<T>(private val variables: MutableMap<String, Any?>) {
    @Suppress("UNCHECKED_CAST")
    operator fun getValue(thisRef: Any?, property: KProperty<*>): T? =
        variables[property.name] as T?

    operator fun setValue(thisRef: Any?, property: KProperty<*>, value: T?) {
        variables[property.name] = value
    }
}

fun <T> required(variables: MutableMap<String, Any?>): RequiredVar<T> = RequiredVar(variables)

fun <T> optional(variables: MutableMap<String, Any?>): OptionalVar<T> = OptionalVar(variables)
