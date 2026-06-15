package loadshift.camunda8

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class Camunda8ControlTest {

    @Test
    fun backendExposesControl() = runTest {
        val backend = Camunda8Backend()
        assertEquals("camunda8", backend.control.backendType)
        assertEquals(emptyList(), backend.control.runs())
    }
}
