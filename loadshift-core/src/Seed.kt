package loadshift.core

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

typealias Seed<W> = suspend () -> Flow<W>

data class Page<C>(val items: List<C>, val next: Any?)

fun interface Paginated<W, C> {
    suspend fun page(input: W, cursor: Any?): Page<C>
}

fun <C> paginate(fetch: suspend (cursor: Any?) -> Page<C>): Paginated<Unit, C> =
    Paginated { _, cursor -> fetch(cursor) }

fun <W, C> paginateWith(fetch: suspend (input: W, cursor: Any?) -> Page<C>): Paginated<W, C> =
    Paginated { input, cursor -> fetch(input, cursor) }

fun <W, C> Paginated<W, C>.asFlow(input: W): Flow<C> = flow {
    var cursor: Any? = null
    do {
        val page = page(input, cursor)
        for (item in page.items) emit(item)
        cursor = page.next
    } while (cursor != null)
}
