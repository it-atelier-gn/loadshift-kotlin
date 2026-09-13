package loadshift.camunda8

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.forms.submitForm
import io.ktor.http.isSuccess
import io.ktor.http.parameters
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

sealed interface Camunda8Auth {
    data object None : Camunda8Auth

    class Bearer(val token: String) : Camunda8Auth {
        override fun toString(): String = "Bearer(token=***)"
    }

    class ClientCredentials(
        val tokenUrl: String,
        val clientId: String,
        val clientSecret: String,
        val audience: String? = null,
        val scope: String? = null,
    ) : Camunda8Auth {
        override fun toString(): String = "ClientCredentials(tokenUrl=$tokenUrl, clientId=$clientId)"
    }
}

@Serializable
internal class TokenResponse(
    @SerialName("access_token") val accessToken: String,
    @SerialName("expires_in") val expiresIn: Long = DEFAULT_EXPIRY_SECONDS,
)

internal class TokenCache(
    private val http: HttpClient,
    private val credentials: Camunda8Auth.ClientCredentials,
    private val now: () -> Instant = { Clock.System.now() },
) {
    private val mutex = Mutex()
    private var token: String? = null
    private var validUntil: Instant = Instant.DISTANT_PAST

    suspend fun token(): String = mutex.withLock {
        token?.takeIf { now() < validUntil }?.let { return it }
        val response = http.submitForm(
            url = credentials.tokenUrl,
            formParameters = parameters {
                append("grant_type", "client_credentials")
                append("client_id", credentials.clientId)
                append("client_secret", credentials.clientSecret)
                credentials.audience?.let { append("audience", it) }
                credentials.scope?.let { append("scope", it) }
            },
        )
        check(response.status.isSuccess()) { "token request to ${credentials.tokenUrl} failed: ${response.status}" }
        val issued = response.body<TokenResponse>()
        token = issued.accessToken
        validUntil = now() + (issued.expiresIn - REFRESH_MARGIN_SECONDS).coerceAtLeast(0).seconds
        issued.accessToken
    }

    suspend fun invalidate() {
        mutex.withLock { token = null }
    }
}

private const val DEFAULT_EXPIRY_SECONDS = 300L
private const val REFRESH_MARGIN_SECONDS = 30L
