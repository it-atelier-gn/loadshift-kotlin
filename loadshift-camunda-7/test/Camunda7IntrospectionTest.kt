package loadshift.camunda7

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class Camunda7IntrospectionTest {

    @Test
    fun backendExposesIntrospection() = runTest {
        val backend = Camunda7Backend()
        assertEquals("camunda7", backend.introspection.backendType)
        assertEquals(emptyList(), backend.introspection.runs())
    }
}
