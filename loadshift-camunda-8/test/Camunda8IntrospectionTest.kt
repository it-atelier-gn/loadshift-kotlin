package loadshift.camunda8

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class Camunda8IntrospectionTest {

    @Test
    fun backendExposesIntrospection() = runTest {
        val backend = Camunda8Backend()
        assertEquals("camunda8", backend.introspection.backendType)
        assertEquals(emptyList(), backend.introspection.runs())
    }
}
