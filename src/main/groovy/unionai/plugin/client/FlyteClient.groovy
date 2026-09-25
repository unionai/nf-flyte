package unionai.plugin.client

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Minimal Flyte v2 client speaking the Connect protocol with JSON bodies, so no
 * generated protobuf stubs are needed.
 *
 * Auth follows the Flyte SDK: the API key is base64("endpoint:client_id:client_secret:org"),
 * exchanged for a bearer token with the OAuth2 client-credentials grant, and sent in the
 * header named by the server's public client config (e.g. `flyte-authorization`).
 */
@Slf4j
@CompileStatic
class FlyteClient {

    static final Set<String> TERMINAL_PHASES = [
        'ACTION_PHASE_SUCCEEDED', 'ACTION_PHASE_FAILED', 'ACTION_PHASE_ABORTED', 'ACTION_PHASE_TIMED_OUT'
    ] as Set

    private static final int MAX_ATTEMPTS = 5
    private static final int LIST_PAGE_SIZE = 500

    private final String baseUrl
    private final String apiKey
    private final String tokenCommand
    private final HttpClient http

    private String authHeader
    private String token
    private Instant tokenExpiry = Instant.EPOCH

    /**
     * @param baseUrl scheme and host of the Flyte API, e.g. `https://tenant.hosted.unionai.cloud`
     * @param apiKey Flyte API key
     * @param tokenCommand command printing an access token, used instead of the API key
     */
    FlyteClient(String baseUrl, String apiKey, String tokenCommand = null) {
        this.baseUrl = baseUrl.replaceFirst(/\/+$/, '')
        this.apiKey = apiKey
        this.tokenCommand = tokenCommand
        this.http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build()
    }

    /**
     * Enqueue an action (`flyteidl2.actions.ActionsService/Enqueue`).
     *
     * @param action a JSON-shaped `flyteidl2.actions.Action`
     */
    void enqueue(Map action) {
        final id = action.actionId as Map
        try {
            call('flyteidl2.actions.ActionsService/Enqueue', [action: action], routing(id, action.parentActionName as String))
        }
        catch( AlreadyExistsException e ) {
            // action names come from the task hash: when a retried Nextflow head resubmits a task
            // that was still running (or done) under the failed attempt, attach to that action
            log.debug "[FLYTE] action ${id.name} already exists, attaching to it"
        }
    }

    void abort(Map actionId, String reason, String parentActionName) {
        call('flyteidl2.actions.ActionsService/Abort', [actionId: actionId, reason: reason], routing(actionId, parentActionName))
    }

    /**
     * The Actions service routes every call by these headers and rejects calls without them.
     */
    static Map<String,String> routing(Map actionId, String parentActionName) {
        final run = actionId.run as Map
        return [
            'x-actions-project': run?.project as String,
            'x-actions-domain': run?.domain as String,
            'x-actions-run': run?.name as String,
            'x-actions-parent-action': parentActionName,
        ]
    }

    /**
     * @return the `flyteidl2.workflow.ActionDetails` of the action, or null when it is not
     *      readable yet (enqueue is acknowledged before the action is visible to the run service)
     */
    Map getActionDetails(Map actionId) {
        try {
            final resp = call('flyteidl2.workflow.RunService/GetActionDetails', [actionId: actionId])
            return resp.details as Map
        }
        catch( NotFoundException e ) {
            return null
        }
    }

    /**
     * Create a run (`flyteidl2.workflow.RunService/CreateRun`).
     *
     * @return the created run (`flyteidl2.workflow.Run`)
     */
    Map createRun(Map request) {
        return call('flyteidl2.workflow.RunService/CreateRun', request).run as Map
    }

    /**
     * Abort a whole run (`flyteidl2.workflow.RunService/AbortRun`): the run and its actions end ABORTED.
     */
    void abortRun(Map runId, String reason) {
        call('flyteidl2.workflow.RunService/AbortRun', [runId: runId, reason: reason])
    }

    /**
     * @return where the action's inputs and outputs are stored (`inputsUri`, `outputsUri`)
     */
    Map getActionDataUris(Map actionId) {
        return call('flyteidl2.workflow.RunService/GetActionDataURIs', [actionId: actionId])
    }

    /**
     * @return the action's inputs and outputs (`flyteidl2.workflow.GetActionDataResponse`)
     */
    Map getActionData(Map actionId) {
        return call('flyteidl2.workflow.RunService/GetActionData', [actionId: actionId])
    }

    /**
     * @return the URI of the named File / Dir literal in a JSON-shaped `Inputs` or `Outputs`
     */
    static String blobUri(Map literals, String name) {
        for( Object it : (literals?.literals ?: []) as List ) {
            final named = it as Map
            if( named.name == name )
                return ((((named.value as Map)?.scalar as Map)?.blob as Map)?.uri) as String
        }
        return null
    }

    /**
     * List the phase of every action in a run.
     *
     * @return map of action name to phase, e.g. `ACTION_PHASE_RUNNING`
     */
    Map<String,String> listActionPhases(Map runId) {
        final result = new HashMap<String,String>()
        String pageToken = ''
        do {
            final resp = call('flyteidl2.workflow.RunService/ListActions',
                [runId: runId, request: [limit: LIST_PAGE_SIZE, token: pageToken]])
            for( Object it : (resp.actions ?: []) as List ) {
                final action = it as Map
                final name = (action.id as Map)?.name as String
                final phase = (action.status as Map)?.phase as String
                if( name )
                    result.put(name, phase ?: 'ACTION_PHASE_UNSPECIFIED')
            }
            pageToken = resp.token as String ?: ''
        }
        while( pageToken )
        return result
    }

    // ------------------------------------------------------------------ transport

    protected Map call(String method, Map body, Map<String,String> headers = [:]) {
        final payload = JsonOutput.toJson(body)
        for( int attempt = 1; ; attempt++ ) {
            final builder = HttpRequest.newBuilder(URI.create("$baseUrl/$method"))
                .timeout(Duration.ofSeconds(30))
                .header('Content-Type', 'application/json')
                .header('Connect-Protocol-Version', '1')
                .header(authHeaderName(), "Bearer ${bearerToken()}")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
            headers.each { k, v -> if( v ) builder.header(k, v) }

            HttpResponse<String> resp
            try {
                resp = http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
            }
            catch( IOException e ) {
                if( attempt >= MAX_ATTEMPTS )
                    throw new FlyteException("$method failed: ${e.message}", e)
                backoff(attempt)
                continue
            }

            final code = resp.statusCode()
            if( code == 200 )
                return (resp.body() ? new JsonSlurper().parseText(resp.body()) : [:]) as Map
            if( code == 401 && attempt == 1 ) {
                // token revoked or expired early: refresh once
                invalidateToken()
                continue
            }
            if( code == 404 )
                throw new NotFoundException("$method: ${resp.body()}")
            if( code == 409 && resp.body()?.contains('already_exists') )
                throw new AlreadyExistsException("$method: ${resp.body()}")
            if( (code == 429 || code >= 500) && attempt < MAX_ATTEMPTS ) {
                log.debug "[FLYTE] $method -> HTTP $code, retrying (attempt $attempt)"
                backoff(attempt)
                continue
            }
            throw new FlyteException("$method -> HTTP $code: ${resp.body()}")
        }
    }

    private static void backoff(int attempt) {
        sleep(Math.min(200L << attempt, 5_000L))
    }

    // ------------------------------------------------------------------ auth

    protected synchronized String authHeaderName() {
        if( !authHeader ) {
            final cfg = anonymous('flyteidl2.auth.AuthMetadataService/GetPublicClientConfig')
            authHeader = cfg.authorizationMetadataKey as String ?: 'authorization'
        }
        return authHeader
    }

    protected synchronized String bearerToken() {
        if( token && Instant.now().isBefore(tokenExpiry) )
            return token
        if( tokenCommand )
            return commandToken()

        final creds = decodeApiKey(apiKey)
        final meta = anonymous('flyteidl2.auth.AuthMetadataService/GetOAuth2Metadata')
        final pub = anonymous('flyteidl2.auth.AuthMetadataService/GetPublicClientConfig')
        final form = new StringBuilder('grant_type=client_credentials&scope=').append(enc('all'))
        if( pub.audience )
            form.append('&audience=').append(enc(pub.audience as String))
        final basic = Base64.encoder.encodeToString("${enc(creds.clientId)}:${enc(creds.clientSecret)}".getBytes(StandardCharsets.UTF_8))

        final req = HttpRequest.newBuilder(URI.create(meta.tokenEndpoint as String))
            .timeout(Duration.ofSeconds(30))
            .header('Content-Type', 'application/x-www-form-urlencoded')
            .header('Authorization', "Basic $basic")
            .POST(HttpRequest.BodyPublishers.ofString(form.toString()))
            .build()
        final resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        if( resp.statusCode() != 200 )
            throw new FlyteException("Unable to get a Flyte access token -> HTTP ${resp.statusCode()}: ${resp.body()}")

        final json = new JsonSlurper().parseText(resp.body()) as Map
        token = json.access_token as String
        final expiresIn = (json.expires_in ?: 3600) as long
        // refresh a minute early
        tokenExpiry = Instant.now().plusSeconds(Math.max(expiresIn - 60, 30))
        log.debug "[FLYTE] obtained access token, expires in ${expiresIn}s"
        return token
    }

    /**
     * Run the token command; its output is the token. It's reused until it expires or the
     * server rejects it: the lifetime isn't known, so assume a conservative 5 minutes.
     */
    private String commandToken() {
        final proc = new ProcessBuilder('bash', '-c', tokenCommand).redirectError(ProcessBuilder.Redirect.INHERIT).start()
        final out = proc.inputStream.text.trim()
        if( proc.waitFor() != 0 || !out )
            throw new FlyteException("Flyte auth command failed (exit ${proc.exitValue()}): $tokenCommand")
        token = out.readLines().last().trim()
        tokenExpiry = Instant.now().plusSeconds(300)
        return token
    }

    protected synchronized void invalidateToken() {
        token = null
        tokenExpiry = Instant.EPOCH
    }

    private Map anonymous(String method) {
        final req = HttpRequest.newBuilder(URI.create("$baseUrl/$method"))
            .timeout(Duration.ofSeconds(30))
            .header('Content-Type', 'application/json')
            .POST(HttpRequest.BodyPublishers.ofString('{}'))
            .build()
        final resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        if( resp.statusCode() != 200 )
            throw new FlyteException("$method -> HTTP ${resp.statusCode()}: ${resp.body()}")
        return new JsonSlurper().parseText(resp.body()) as Map
    }

    static ApiKey decodeApiKey(String key) {
        if( !key )
            throw new FlyteException('Missing Flyte API key')
        // the endpoint may itself contain colons (e.g. dns:///host), so split from the right
        final parts = new String(Base64.decoder.decode(key.trim()), StandardCharsets.UTF_8).split(':', -1)
        if( parts.length < 4 )
            throw new FlyteException("Invalid Flyte API key: expected 'endpoint:client_id:client_secret:org'")
        final endpoint = parts[0..parts.length - 4].join(':')
        final org = parts[parts.length - 1]
        return new ApiKey(parts[parts.length - 3], parts[parts.length - 2], endpoint, org && org != 'None' ? org : null)
    }

    private static String enc(String s) {
        URLEncoder.encode(s, StandardCharsets.UTF_8)
    }

    @CompileStatic
    static class ApiKey {
        final String clientId
        final String clientSecret
        final String endpoint
        final String org

        ApiKey(String clientId, String clientSecret, String endpoint = null, String org = null) {
            this.clientId = clientId
            this.clientSecret = clientSecret
            this.endpoint = endpoint
            this.org = org
        }
    }
}
