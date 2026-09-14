package loadshift.camunda8.stream

import io.camunda.client.CamundaClient
import io.camunda.client.api.CamundaFuture
import io.camunda.client.api.command.StreamJobsCommandStep1
import io.camunda.client.api.response.ActivatedJob
import kotlinx.serialization.json.JsonPrimitive
import loadshift.camunda8.StreamedJob
import java.lang.reflect.Proxy
import java.util.function.BiConsumer
import java.util.function.Consumer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class CamundaJobStreamTest {

    private class Recorded {
        val calls = mutableMapOf<String, Any?>()
        var consumer: Consumer<ActivatedJob>? = null
        var completion: BiConsumer<Any?, Throwable?>? = null
        var cancelled = false
        var clientClosed = false
    }

    private inline fun <reified T> proxy(crossinline handler: (name: String, args: Array<Any?>) -> Any?): T =
        Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { _, method, args ->
            handler(method.name, args ?: emptyArray())
        } as T

    private fun client(recorded: Recorded): CamundaClient {
        val future = proxy<CamundaFuture<*>> { name, args ->
            when (name) {
                "cancel" -> {
                    recorded.cancelled = true
                    true
                }
                "whenComplete" -> {
                    @Suppress("UNCHECKED_CAST")
                    recorded.completion = args.single() as BiConsumer<Any?, Throwable?>
                    null
                }
                else -> null
            }
        }
        lateinit var step3: StreamJobsCommandStep1.StreamJobsCommandStep3
        step3 = proxy { name, args ->
            when (name) {
                "send" -> future
                else -> {
                    recorded.calls[name] = args.firstOrNull()
                    step3
                }
            }
        }
        val step2 = proxy<StreamJobsCommandStep1.StreamJobsCommandStep2> { _, args ->
            @Suppress("UNCHECKED_CAST")
            recorded.consumer = args.single() as Consumer<ActivatedJob>
            step3
        }
        val step1 = proxy<StreamJobsCommandStep1> { _, args ->
            recorded.calls["jobType"] = args.single()
            step2
        }
        return proxy { name, _ ->
            when (name) {
                "newStreamJobsCommand" -> step1
                "close" -> {
                    recorded.clientClosed = true
                    null
                }
                else -> error("unexpected call $name")
            }
        }
    }

    private fun job(variables: String?) = proxy<ActivatedJob> { name, _ ->
        when (name) {
            "getKey" -> 42L
            "getType" -> "wf/pack"
            "getProcessInstanceKey" -> 7L
            "getRetries" -> 3
            "getVariables" -> variables
            else -> error("unexpected call $name")
        }
    }

    @Test
    fun opensAStreamPerJobTypeConvertsPushedJobsAndReportsItsEnd() {
        val recorded = Recorded()
        val received = mutableListOf<StreamedJob>()
        val endings = mutableListOf<Throwable?>()
        val stream = CamundaJobStream(client(recorded))

        val handle = stream.open("wf/pack", "worker-1", 30.seconds, "acme", { received += it }) { endings += it }
        recorded.consumer!!.accept(job("""{"id":"p1","packed":false}"""))
        recorded.consumer!!.accept(job(null))
        val failure = IllegalStateException("UNAVAILABLE")
        recorded.completion!!.accept(null, failure)
        handle.close()
        recorded.completion!!.accept(null, null)
        stream.close()

        assertEquals("wf/pack", recorded.calls["jobType"])
        assertEquals("worker-1", recorded.calls["workerName"])
        assertEquals(java.time.Duration.ofSeconds(30), recorded.calls["timeout"])
        assertEquals("acme", recorded.calls["tenantId"])
        assertEquals(listOf("42", "42"), received.map { it.jobKey })
        assertEquals("7", received.first().processInstanceKey)
        assertEquals(3, received.first().retries)
        assertEquals(JsonPrimitive("p1"), received.first().variables["id"])
        assertTrue(received.last().variables.isEmpty())
        assertEquals(listOf<Throwable?>(failure), endings.toList())
        assertTrue(recorded.cancelled)
        assertEquals(false, recorded.clientClosed)
    }
}
