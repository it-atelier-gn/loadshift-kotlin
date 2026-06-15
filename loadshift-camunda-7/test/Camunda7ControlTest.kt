package loadshift.camunda7

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class Camunda7ControlTest {

    @Test
    fun backendExposesControl() = runTest {
        val backend = Camunda7Backend()
        assertEquals("camunda7", backend.control.backendType)
        assertEquals(emptyList(), backend.control.runs())
    }
}
