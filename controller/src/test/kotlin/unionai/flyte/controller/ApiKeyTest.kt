package unionai.flyte.controller

import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class ApiKeyTest {

    @Test
    fun `decodes an API key`() {
        val key = Base64.getEncoder().encodeToString("dns:///tenant.hosted.unionai.cloud:my-id:my-secret:acme".toByteArray())
        val decoded = ApiKey.decode(key)
        assertEquals("my-id", decoded.clientId)
        assertEquals("my-secret", decoded.clientSecret)
        assertEquals("dns:///tenant.hosted.unionai.cloud", decoded.endpoint)
        assertEquals("acme", decoded.org)

        val noOrg = Base64.getEncoder().encodeToString("host:id:secret:None".toByteArray())
        assertNull(ApiKey.decode(noOrg).org)
        assertFailsWith<FlyteException> { ApiKey.decode(Base64.getEncoder().encodeToString("nope".toByteArray())) }
        assertFailsWith<FlyteException> { ApiKey.decode("not base64!") }
    }
}
