package unionai.flyte.controller.internal

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import mockwebserver3.MockResponse
import mockwebserver3.MockWebServer
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import okio.BufferedSink
import unionai.flyte.controller.FlyteException
import unionai.flyte.controller.TokenSource

class AuthTest {

    /** Hands out tok-1, tok-2, ...: a new token on every fetch after an invalidate */
    private class CountingTokens : TokenSource {
        val fetches = AtomicInteger()
        private val cached = CachedToken()

        @Synchronized
        override fun token(): String = cached.valid() ?: cached.set("tok-${fetches.incrementAndGet()}", java.time.Instant.MAX)

        @Synchronized
        override fun invalidate(rejected: String) = cached.invalidate(rejected)
    }

    private fun client(tokens: TokenSource) =
        OkHttpClient.Builder().addInterceptor(AuthInterceptor(tokens) { "flyte-authorization" }).build()

    @Test
    fun `retries a rejected call once with a fresh token`() {
        MockWebServer().use { server ->
            server.enqueue(MockResponse.Builder().code(401).build())
            server.enqueue(MockResponse.Builder().code(200).body("ok").build())
            server.start()
            val tokens = CountingTokens()

            val body = client(tokens).newCall(Request.Builder().url(server.url("/x")).post("{}".toRequestBody()).build()).execute().use { it.body.string() }

            assertEquals("ok", body)
            assertEquals("Bearer tok-1", server.takeRequest().headers["flyte-authorization"])
            assertEquals("Bearer tok-2", server.takeRequest().headers["flyte-authorization"])
        }
    }

    @Test
    fun `does not replay a body that can only be sent once`() {
        MockWebServer().use { server ->
            server.enqueue(MockResponse.Builder().code(401).build())
            server.start()
            val oneShot = object : RequestBody() {
                override fun contentType() = "application/connect+proto".toMediaType()
                override fun isOneShot() = true
                override fun writeTo(sink: BufferedSink) { sink.writeUtf8("x") }
            }

            val code = client(CountingTokens()).newCall(Request.Builder().url(server.url("/x")).post(oneShot).build()).execute().use { it.code }

            assertEquals(401, code)
            assertEquals(1, server.requestCount)
        }
    }

    @Test
    fun `only drops the token that was rejected`() {
        val tokens = CountingTokens()
        val first = tokens.token()
        tokens.invalidate(first)
        val second = tokens.token()
        // a second call rejected with the first token must not drop the one already replacing it
        tokens.invalidate(first)
        assertEquals(second, tokens.token())
        assertEquals(2, tokens.fetches.get())
    }

    @Test
    fun `reports a failure to get a token as an IO error, so it is retried`() {
        val failing = object : TokenSource {
            override fun token(): String = throw FlyteException("token endpoint returned 503")
            override fun invalidate(rejected: String) {}
        }
        MockWebServer().use { server ->
            server.start()
            val e = assertFailsWith<IOException> {
                client(failing).newCall(Request.Builder().url(server.url("/x")).build()).execute()
            }
            assertEquals(FlyteException::class, e.cause!!::class)
        }
    }

    @Test
    fun `runs the auth command and caches its token`() {
        val tokens = CommandTokenSource("echo noise; echo my-token")
        assertEquals("my-token", tokens.token())
        assertEquals("my-token", tokens.token())
        assertFailsWith<FlyteException> { CommandTokenSource("exit 3").token() }
    }
}
