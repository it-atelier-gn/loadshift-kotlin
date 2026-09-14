package loadshift.web

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.cookies.HttpCookies
import io.ktor.client.request.bearerAuth
import io.ktor.client.request.get
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.encodeURLParameter
import io.ktor.server.engine.embeddedServer
import io.ktor.server.request.receiveParameters
import io.ktor.server.response.respond
import io.ktor.server.response.respondRedirect
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.coroutines.runBlocking
import loadshift.local.LocalBackend
import java.math.BigInteger
import java.net.ServerSocket
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.interfaces.RSAPrivateKey
import java.security.interfaces.RSAPublicKey
import java.util.Base64
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ConsoleOidcTest {

    private fun keyPair(): KeyPair = KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair()

    private val signing = keyPair()

    private fun token(
        issuer: String,
        audience: String = "console",
        groups: List<String> = listOf("ops"),
        keys: KeyPair = signing,
    ): String = JWT.create()
        .withKeyId("k1")
        .withIssuer(issuer)
        .withAudience(audience)
        .withSubject("alice")
        .withClaim("groups", groups)
        .withExpiresAt(Date(System.currentTimeMillis() + 300_000))
        .sign(Algorithm.RSA256(keys.public as RSAPublicKey, keys.private as RSAPrivateKey))

    private fun freePort(): Int = ServerSocket(0).use { it.localPort }

    private fun unsigned(value: BigInteger): String {
        val bytes = value.toByteArray().let { if (it[0] == 0.toByte()) it.copyOfRange(1, it.size) else it }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
    }

    private fun identityProvider(port: Int, idToken: () -> String) =
        embeddedServer(io.ktor.server.cio.CIO, port = port, host = "127.0.0.1") {
            val base = "http://127.0.0.1:$port"
            routing {
                get("/.well-known/openid-configuration") {
                    call.respondText(
                        """{"issuer":"$base","authorization_endpoint":"$base/authorize","token_endpoint":"$base/token","jwks_uri":"$base/jwks"}""",
                        ContentType.Application.Json,
                    )
                }
                get("/jwks") {
                    val key = signing.public as RSAPublicKey
                    call.respondText(
                        """{"keys":[{"kty":"RSA","kid":"k1","use":"sig","alg":"RS256","n":"${unsigned(key.modulus)}","e":"${unsigned(key.publicExponent)}"}]}""",
                        ContentType.Application.Json,
                    )
                }
                get("/authorize") {
                    val redirect = call.request.queryParameters["redirect_uri"].orEmpty()
                    val state = call.request.queryParameters["state"].orEmpty()
                    call.respondRedirect("$redirect?code=granted&state=${state.encodeURLParameter()}")
                }
                post("/token") {
                    if (call.receiveParameters()["code"] != "granted") return@post call.respond(HttpStatusCode.BadRequest)
                    call.respondText(
                        """{"access_token":"access","token_type":"Bearer","expires_in":300,"id_token":"${idToken()}"}""",
                        ContentType.Application.Json,
                    )
                }
            }
        }.start()

    @Test
    fun browserLoginOpensASessionAndBearerTokensReachTheApi() = runBlocking {
        val idpPort = freePort()
        val consolePort = freePort()
        val issuer = "http://127.0.0.1:$idpPort"
        val console = "http://127.0.0.1:$consolePort"
        var idToken = token(issuer)
        val idp = identityProvider(idpPort) { idToken }
        val oidc = ConsoleOidc(
            issuer = issuer,
            clientId = "console",
            clientSecret = "client-secret",
            redirectUrl = "$console/login/callback",
            requiredClaims = mapOf("groups" to "ops"),
            secureCookie = false,
        )
        val server = ControlServer(LocalBackend(), port = consolePort, oidc = oidc).start()
        val browser = HttpClient(CIO) {
            followRedirects = false
            install(HttpCookies)
        }
        val api = HttpClient(CIO) { followRedirects = false }
        try {
            assertEquals(401, browser.get("$console/api/runs").status.value)
            val home = browser.get("$console/")
            assertEquals(302, home.status.value)
            assertEquals("/login", home.headers[HttpHeaders.Location])

            val authorize = assertNotNull(browser.get("$console/login").headers[HttpHeaders.Location])
            assertTrue(authorize.startsWith("$issuer/authorize"), authorize)
            val callback = assertNotNull(browser.get(authorize).headers[HttpHeaders.Location])
            val landed = browser.get(callback)
            assertEquals(302, landed.status.value)
            assertEquals("/", landed.headers[HttpHeaders.Location])
            assertEquals(200, browser.get("$console/api/runs").status.value)

            browser.get("$console/logout")
            assertEquals(401, browser.get("$console/api/runs").status.value)

            assertEquals(200, api.get("$console/api/runs") { bearerAuth(token(issuer)) }.status.value)
            assertEquals(401, api.get("$console/api/runs") { bearerAuth(token(issuer, groups = listOf("dev"))) }.status.value)
            assertEquals(401, api.get("$console/api/runs") { bearerAuth(token(issuer, audience = "other")) }.status.value)
            assertEquals(401, api.get("$console/api/runs") { bearerAuth(token("http://127.0.0.1:1")) }.status.value)
            assertEquals(401, api.get("$console/api/runs") { bearerAuth(token(issuer, keys = keyPair())) }.status.value)

            val forged = api.get("$console/login/callback?code=granted&state=forged")
            assertFalse(forged.headers.getAll(HttpHeaders.SetCookie).orEmpty().any { it.startsWith("loadshift_session=") })

            idToken = token(issuer, groups = listOf("dev"))
            val outsider = HttpClient(CIO) {
                followRedirects = false
                install(HttpCookies)
            }
            try {
                val next = assertNotNull(outsider.get("$console/login").headers[HttpHeaders.Location])
                val back = assertNotNull(outsider.get(next).headers[HttpHeaders.Location])
                assertEquals(403, outsider.get(back).status.value)
                assertEquals(401, outsider.get("$console/api/runs").status.value)
            } finally {
                outsider.close()
            }
        } finally {
            browser.close()
            api.close()
            server.stop()
            idp.stop(100, 1000)
        }
    }

    @Test
    fun oidcConfigurationIsValidatedAndKeepsTheSecretOutOfToString() {
        val oidc = ConsoleOidc("https://idp.example", "console", "top-secret", "https://console.example/login/callback")

        assertFalse("top-secret" in oidc.toString())
        assertFailsWith<IllegalArgumentException> {
            ControlServer(LocalBackend(), credentials = ConsoleCredentials("ops", "pw"), oidc = oidc)
        }
        assertFailsWith<IllegalArgumentException> {
            ConsoleOidc("https://idp.example", "console", "s", "https://console.example/cb", scopes = listOf("profile"))
        }
        assertFailsWith<IllegalArgumentException> {
            ConsoleOidc("https://idp.example", "console", "s", "https://console.example/cb", sessionKey = ByteArray(8))
        }
    }
}
