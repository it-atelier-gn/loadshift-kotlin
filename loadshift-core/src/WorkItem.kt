package loadshift.core

import kotlin.reflect.KProperty

abstract class WorkItemBase {
    open val key: String? = null

    private val variables: MutableMap<String, Any?> = mutableMapOf()

    fun toMap(): Map<String, Any?> = variables.toMap()

    internal fun hydrate(values: Map<String, Any?>) {
        variables.putAll(values)
    }

    protected fun <T> required(default: T? = null): RequiredVar<T> = RequiredVar(variables, default)

    protected fun <T> optional(): OptionalVar<T> = OptionalVar(variables)
}

class RequiredVar<T> internal constructor(
    private val variables: MutableMap<String, Any?>,
    private val default: T? = null,
) {
    @Suppress("UNCHECKED_CAST")
    operator fun getValue(thisRef: Any?, property: KProperty<*>): T =
        (variables[property.name] as T?)
            ?: default
            ?: error("Missing required variable '${property.name}'")

    operator fun setValue(thisRef: Any?, property: KProperty<*>, value: T) {
        variables[property.name] = value
    }
}

class OptionalVar<T> internal constructor(private val variables: MutableMap<String, Any?>) {
    @Suppress("UNCHECKED_CAST")
    operator fun getValue(thisRef: Any?, property: KProperty<*>): T? =
        variables[property.name] as T?

    operator fun setValue(thisRef: Any?, property: KProperty<*>, value: T?) {
        variables[property.name] = value
    }
}
