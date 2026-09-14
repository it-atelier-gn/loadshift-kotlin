package loadshift.web

import com.auth0.jwk.JwkProviderBuilder
import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.DecodedJWT
import com.auth0.jwt.interfaces.JWTVerifier
import com.auth0.jwt.interfaces.Payload
import com.auth0.jwt.interfaces.RSAKeyProvider
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.get
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.Url
import io.ktor.http.isSuccess
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.auth.Authentication
import io.ktor.server.auth.OAuthAccessTokenResponse
import io.ktor.server.auth.OAuthServerSettings
import io.ktor.server.auth.authenticate
import io.ktor.server.auth.jwt.JWTPrincipal
import io.ktor.server.auth.jwt.jwt
import io.ktor.server.auth.oauth
import io.ktor.server.auth.principal
import io.ktor.server.auth.session
import io.ktor.server.request.path
import io.ktor.server.response.respond
import io.ktor.server.response.respondRedirect
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.sessions.SessionTransportTransformerEncrypt
import io.ktor.server.sessions.Sessions
import io.ktor.server.sessions.clear
import io.ktor.server.sessions.cookie
import io.ktor.server.sessions.sessions
import io.ktor.server.sessions.set
import io.ktor.util.StatelessHmacNonceManager
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.net.URI
import java.security.SecureRandom
import java.security.interfaces.RSAPrivateKey
import java.security.interfaces.RSAPublicKey
import java.util.concurrent.TimeUnit
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

class ConsoleOidc(
    val issuer: String,
    val clientId: String,
    val clientSecret: String,
    val redirectUrl: String,
    val scopes: List<String> = listOf("openid", "profile", "email"),
    val requiredClaims: Map<String, String> = emptyMap(),
    val sessionTtl: Duration = 8.hours,
    val secureCookie: Boolean = true,
    val sessionKey: ByteArray? = null,
) {
    init {
        require("openid" in scopes) { "scopes must contain openid" }
        require(sessionTtl.isPositive()) { "sessionTtl must be positive, was $sessionTtl" }
        require(sessionKey == null || sessionKey.size >= MIN_KEY_BYTES) { "sessionKey must have at least $MIN_KEY_BYTES bytes" }
    }

    override fun toString(): String = "ConsoleOidc(issuer=$issuer, clientId=$clientId, redirectUrl=$redirectUrl)"

    private companion object {
        const val MIN_KEY_BYTES = 32
    }
}

@Serializable
internal data class ConsoleSession(val subject: String, val expiresAt: Long)

@Serializable
private data class DiscoveryDocument(
    val issuer: String,
    @SerialName("authorization_endpoint") val authorizationEndpoint: String,
    @SerialName("token_endpoint") val tokenEndpoint: String,
    @SerialName("jwks_uri") val jwksUri: String,
)

internal class OidcProvider private constructor(
    val config: ConsoleOidc,
    val client: HttpClient,
    val settings: OAuthServerSettings.OAuth2ServerSettings,
    val verifier: JWTVerifier,
    private val sessionKey: ByteArray,
) : AutoCloseable {

    val callbackPath: String = Url(config.redirectUrl).encodedPath.ifEmpty { "/" }

    fun decode(token: String): DecodedJWT? = try {
        verifier.verify(token)
    } catch (e: Exception) {
        null
    }

    fun authorizes(payload: Payload): Boolean = config.requiredClaims.all { (name, value) ->
        val claim = payload.getClaim(name)
        !claim.isMissing && !claim.isNull &&
            (claim.asString() == value || runCatching { claim.asList(String::class.java) }.getOrNull()?.contains(value) == true)
    }

    fun key(label: String, bytes: Int): ByteArray {
        val mac = Mac.getInstance(HMAC)
        mac.init(SecretKeySpec(sessionKey, HMAC))
        return mac.doFinal(label.toByteArray(Charsets.UTF_8)).copyOf(bytes)
    }

    override fun close() {
        client.close()
    }

    companion object {
        private const val HMAC = "HmacSHA256"
        private const val LEEWAY_SECONDS = 30L

        suspend fun discover(config: ConsoleOidc): OidcProvider {
            val client = HttpClient(CIO) {
                install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
            }
            try {
                val response = client.get("${config.issuer.trimEnd('/')}/.well-known/openid-configuration")
                check(response.status.isSuccess()) { "OpenID discovery for '${config.issuer}' failed: ${response.status}" }
                val discovery = response.body<DiscoveryDocument>()
                require(discovery.issuer == config.issuer) {
                    "issuer '${discovery.issuer}' of the discovery document differs from '${config.issuer}'"
                }
                val jwks = JwkProviderBuilder(URI(discovery.jwksUri).toURL())
                    .cached(10, 24, TimeUnit.HOURS)
                    .rateLimited(10, 1, TimeUnit.MINUTES)
                    .build()
                val keys = object : RSAKeyProvider {
                    override fun getPublicKeyById(keyId: String?): RSAPublicKey = try {
                        jwks.get(keyId).publicKey as RSAPublicKey
                    } catch (e: Exception) {
                        throw IllegalStateException("no RSA signing key '$keyId'", e)
                    }

                    override fun getPrivateKey(): RSAPrivateKey? = null

                    override fun getPrivateKeyId(): String? = null
                }
                val verifier = JWT.require(Algorithm.RSA256(keys))
                    .withIssuer(config.issuer)
                    .withAudience(config.clientId)
                    .acceptLeeway(LEEWAY_SECONDS)
                    .build()
                val sessionKey = config.sessionKey ?: ByteArray(32).also { SecureRandom().nextBytes(it) }
                val stateKey = Mac.getInstance(HMAC).run {
                    init(SecretKeySpec(sessionKey, HMAC))
                    doFinal("state".toByteArray(Charsets.UTF_8))
                }
                val settings = OAuthServerSettings.OAuth2ServerSettings(
                    name = "oidc",
                    authorizeUrl = discovery.authorizationEndpoint,
                    accessTokenUrl = discovery.tokenEndpoint,
                    requestMethod = HttpMethod.Post,
                    clientId = config.clientId,
                    clientSecret = config.clientSecret,
                    defaultScopes = config.scopes,
                    nonceManager = StatelessHmacNonceManager(stateKey),
                )
                return OidcProvider(config, client, settings, verifier, sessionKey)
            } catch (e: Throwable) {
                client.close()
                throw e
            }
        }
    }
}

private const val OIDC_LOGIN = "loadshift-oidc-login"
private const val OIDC_SESSION = "loadshift-oidc-session"
private const val OIDC_BEARER = "loadshift-oidc-bearer"
private const val SESSION_COOKIE = "loadshift_session"
private const val LOGIN_PATH = "/login"
private const val LOGOUT_PATH = "/logout"

internal fun Application.installOidc(provider: OidcProvider) {
    install(Sessions) {
        cookie<ConsoleSession>(SESSION_COOKIE) {
            cookie.path = "/"
            cookie.httpOnly = true
            cookie.secure = provider.config.secureCookie
            cookie.extensions["SameSite"] = "Lax"
            transform(
                SessionTransportTransformerEncrypt(
                    SecretKeySpec(provider.key("encrypt", 16), "AES"),
                    SecretKeySpec(provider.key("sign", 32), "HmacSHA256"),
                ),
            )
        }
    }
    install(Authentication) {
        oauth(OIDC_LOGIN) {
            client = provider.client
            providerLookup = { provider.settings }
            urlProvider = { provider.config.redirectUrl }
        }
        session<ConsoleSession>(OIDC_SESSION) {
            validate { session -> session.takeIf { it.expiresAt > Clock.System.now().toEpochMilliseconds() } }
            challenge {
                if (call.request.path().startsWith("/api")) {
                    call.respond(HttpStatusCode.Unauthorized)
                } else {
                    call.respondRedirect(LOGIN_PATH)
                }
            }
        }
        jwt(OIDC_BEARER) {
            verifier(provider.verifier)
            validate { credential -> if (provider.authorizes(credential.payload)) JWTPrincipal(credential.payload) else null }
            challenge { _, _ -> call.respond(HttpStatusCode.Unauthorized) }
        }
    }
}

internal fun Route.oidcRoutes(provider: OidcProvider, routes: Route.() -> Unit) {
    authenticate(OIDC_LOGIN) {
        get(LOGIN_PATH) { }
        get(provider.callbackPath) {
            val tokens = call.principal<OAuthAccessTokenResponse.OAuth2>()
                ?: return@get call.respond(HttpStatusCode.Unauthorized)
            val idToken = tokens.extraParameters["id_token"] ?: return@get call.respond(HttpStatusCode.Unauthorized)
            val verified = provider.decode(idToken) ?: return@get call.respond(HttpStatusCode.Unauthorized)
            if (!provider.authorizes(verified)) return@get call.respond(HttpStatusCode.Forbidden)
            val expiresAt = Clock.System.now() + provider.config.sessionTtl
            call.sessions.set(ConsoleSession(verified.subject, expiresAt.toEpochMilliseconds()))
            call.respondRedirect("/")
        }
    }
    get(LOGOUT_PATH) {
        call.sessions.clear<ConsoleSession>()
        call.respondText("signed out")
    }
    authenticate(OIDC_SESSION, OIDC_BEARER) { routes() }
}
