package unionai.plugin.client

import java.nio.charset.StandardCharsets

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import spock.lang.AutoCleanup
import spock.lang.Specification

/**
 * Runs FlyteClient against an in-process fake of the Flyte Connect API.
 */
class FlyteClientTest extends Specification {

    @AutoCleanup('stop')
    FakeFlyte server = new FakeFlyte()

    static final Map RUN_ACTION = [run: [org: 'acme', project: 'proj', domain: 'dev', name: 'r1'], name: 'nf-a']

    def 'should route aborts like enqueues'() {
        given:
        def client = new FlyteClient(server.url, apiKey())

        when:
        client.abort(RUN_ACTION, 'Killed by Nextflow', 'a0')

        then:
        def req = server.requests['flyteidl2.actions.ActionsService/Abort'][0]
        req.headers['x-actions-project'] == 'proj'
        req.headers['x-actions-domain'] == 'dev'
        req.headers['x-actions-run'] == 'r1'
        req.headers['x-actions-parent-action'] == 'a0'
        req.body == [actionId: RUN_ACTION, reason: 'Killed by Nextflow']
    }

    static String apiKey(String clientId = 'nf-client', String secret = 's3cr+t/') {
        // endpoint contains colons, like the real keys
        Base64.encoder.encodeToString("dns:///tenant.example.com:${clientId}:${secret}:acme".getBytes(StandardCharsets.UTF_8))
    }

    def 'should decode api keys whose endpoint contains colons'() {
        when:
        def key = FlyteClient.decodeApiKey(apiKey('id1', 'sec'))

        then:
        key.clientId == 'id1'
        key.clientSecret == 'sec'
    }

    def 'should reject malformed api keys'() {
        when:
        FlyteClient.decodeApiKey(Base64.encoder.encodeToString('nope'.bytes))

        then:
        thrown(FlyteException)
    }

    def 'should authenticate and enqueue with routing headers'() {
        given:
        def client = new FlyteClient(server.url, apiKey())
        def action = [
            actionId: [run: [org: 'acme', project: 'proj', domain: 'dev', name: 'r1'], name: 'nf-a'],
            parentActionName: 'a0',
            inputUri: 's3://b/in', runOutputBase: 's3://b/base',
            task: [spec: [taskTemplate: [type: 'container']]],
        ]

        when:
        client.enqueue(action)

        then:
        server.tokenRequests == 1
        server.lastTokenForm.contains('grant_type=client_credentials')
        server.lastTokenBasicAuth == Base64.encoder.encodeToString('nf-client:s3cr%2Bt%2F'.bytes)
        def req = server.requests['flyteidl2.actions.ActionsService/Enqueue'][0]
        req.headers['flyte-authorization'] == 'Bearer tok-1'
        req.headers['x-actions-project'] == 'proj'
        req.headers['x-actions-domain'] == 'dev'
        req.headers['x-actions-run'] == 'r1'
        req.headers['x-actions-parent-action'] == 'a0'
        req.body.action.actionId.name == 'nf-a'
    }

    def 'should reuse the token across calls'() {
        given:
        def client = new FlyteClient(server.url, apiKey())

        when:
        3.times { client.abort(RUN_ACTION, 'bye', 'a0') }

        then:
        server.tokenRequests == 1
        server.requests['flyteidl2.actions.ActionsService/Abort'].size() == 3
        server.requests['flyteidl2.actions.ActionsService/Abort'][0].body.reason == 'bye'
    }

    def 'should refresh the token once on 401'() {
        given:
        def client = new FlyteClient(server.url, apiKey())
        server.rejectToken = 'tok-1'

        when:
        client.abort(RUN_ACTION, 'x', 'a0')

        then:
        server.tokenRequests == 2
        server.requests['flyteidl2.actions.ActionsService/Abort'].last().headers['flyte-authorization'] == 'Bearer tok-2'
    }

    def 'should attach to an action that already exists'() {
        given:
        def client = new FlyteClient(server.url, apiKey())
        server.alreadyExists = true

        when:
        client.enqueue([actionId: [run: [project: 'p', domain: 'd', name: 'r1'], name: 'nf-a'], parentActionName: 'a0'])

        then:
        noExceptionThrown()
        server.requests['flyteidl2.actions.ActionsService/Enqueue'].size() == 1
    }

    def 'should not swallow other conflicts'() {
        given:
        def client = new FlyteClient(server.url, apiKey())
        server.alreadyExists = true

        when:
        client.abort(RUN_ACTION, 'x', 'a0')

        then:
        thrown(AlreadyExistsException)
    }

    def 'should create runs and read their data locations'() {
        given:
        def client = new FlyteClient(server.url, apiKey())
        server.createdRun = [action: [id: [run: [name: 'r42']]]]
        server.dataUris = [inputsUri: 's3://b/r42/a0/inputs.pb']

        expect:
        client.createRun([projectId: [organization: 'acme', name: 'p', domain: 'd']]).action.id.run.name == 'r42'
        client.getActionDataUris([run: [name: 'r42'], name: 'a0']).inputsUri == 's3://b/r42/a0/inputs.pb'
        server.requests['flyteidl2.workflow.RunService/CreateRun'][0].body.projectId.organization == 'acme'
    }

    def 'should abort a whole run'() {
        given:
        def client = new FlyteClient(server.url, apiKey())

        when:
        client.abortRun([org: 'acme', project: 'p', domain: 'd', name: 'r42'], 'Cancelled from Nextflow (SIGINT)')

        then:
        server.requests['flyteidl2.workflow.RunService/AbortRun'][0].body == [runId: [org: 'acme', project: 'p', domain: 'd', name: 'r42'], reason: 'Cancelled from Nextflow (SIGINT)']
    }

    def 'should authenticate with a token command'() {
        given:
        def client = new FlyteClient(server.url, null, 'echo some-log-line; echo cmd-token')

        when:
        client.abort(RUN_ACTION, 'x', 'a0')

        then: 'the last line of its output is the token, and no OAuth exchange happens'
        server.tokenRequests == 0
        server.requests['flyteidl2.actions.ActionsService/Abort'][0].headers['flyte-authorization'] == 'Bearer cmd-token'
    }

    def 'should report a failing token command'() {
        when:
        new FlyteClient(server.url, null, 'exit 3').abort(RUN_ACTION, 'x', 'a0')

        then:
        def e = thrown(FlyteException)
        e.message.contains('auth command failed')
    }

    def 'should return null for actions that are not readable yet'() {
        given:
        def client = new FlyteClient(server.url, apiKey())
        server.details['nf-known'] = [status: [phase: 'ACTION_PHASE_FAILED'], errorInfo: [message: 'boom']]

        expect:
        client.getActionDetails([run: [name: 'r1'], name: 'nf-missing']) == null
        client.getActionDetails([run: [name: 'r1'], name: 'nf-known']).errorInfo.message == 'boom'
    }

    def 'should page through ListActions'() {
        given:
        def client = new FlyteClient(server.url, apiKey())
        server.actionPages = [
            [actions: [[id: [name: 'a0'], status: [phase: 'ACTION_PHASE_RUNNING']], [id: [name: 'nf-1'], status: [phase: 'ACTION_PHASE_SUCCEEDED']]], token: 'p2'],
            [actions: [[id: [name: 'nf-2'], status: [:]]]],
        ]

        when:
        def phases = client.listActionPhases([org: 'acme', project: 'proj', domain: 'dev', name: 'r1'])

        then:
        phases == [a0: 'ACTION_PHASE_RUNNING', 'nf-1': 'ACTION_PHASE_SUCCEEDED', 'nf-2': 'ACTION_PHASE_UNSPECIFIED']
        server.requests['flyteidl2.workflow.RunService/ListActions']*.body.request.token == ['', 'p2']
    }

    def 'should retry transient server errors'() {
        given:
        def client = new FlyteClient(server.url, apiKey())
        server.failNext = 2

        when:
        client.abort(RUN_ACTION, 'x', 'a0')

        then:
        server.requests['flyteidl2.actions.ActionsService/Abort'].size() == 3
    }

    def 'should surface client errors'() {
        given:
        def client = new FlyteClient(server.url, apiKey())
        server.badRequest = true

        when:
        client.abort(RUN_ACTION, 'x', 'a0')

        then:
        def e = thrown(FlyteException)
        e.message.contains('HTTP 400')
    }

    /**
     * Just enough of the Flyte API for the client.
     */
    static class FakeFlyte {
        final HttpServer http = HttpServer.create(new InetSocketAddress('127.0.0.1', 0), 0)
        final Map<String, List<Map>> requests = [:].withDefault { [] }
        final Map<String, Map> details = [:]
        List<Map> actionPages = []
        int tokenRequests = 0
        String lastTokenForm
        String lastTokenBasicAuth
        String rejectToken
        int failNext = 0
        boolean badRequest = false
        boolean alreadyExists = false
        Map createdRun = [:]
        Map dataUris = [:]

        FakeFlyte() {
            http.createContext('/') { HttpExchange ex -> handle(ex) }
            http.start()
        }

        String getUrl() { "http://127.0.0.1:${http.address.port}" }

        void stop() { http.stop(0) }

        private Object handle(HttpExchange ex) {
            final path = ex.requestURI.path.substring(1)
            final body = ex.requestBody.text
            switch( path ) {
                case 'flyteidl2.auth.AuthMetadataService/GetOAuth2Metadata':
                    return reply(ex, 200, [tokenEndpoint: "$url/oauth2/token"])
                case 'flyteidl2.auth.AuthMetadataService/GetPublicClientConfig':
                    return reply(ex, 200, [authorizationMetadataKey: 'flyte-authorization', scopes: ['all']])
                case 'oauth2/token':
                    tokenRequests++
                    lastTokenForm = body
                    lastTokenBasicAuth = ex.requestHeaders.getFirst('Authorization') - 'Basic '
                    return reply(ex, 200, [access_token: "tok-$tokenRequests", expires_in: 3600])
            }

            final auth = ex.requestHeaders.getFirst('flyte-authorization')
            final headers = ex.requestHeaders.collectEntries { k, v -> [k.toLowerCase(), v[0]] }
            requests[path] << [headers: headers, body: body ? new JsonSlurper().parseText(body) : [:]]
            if( !auth?.startsWith('Bearer ') || auth == "Bearer $rejectToken" )
                return reply(ex, 401, [code: 'unauthenticated'])
            // like the real service: every Actions call must carry the routing headers
            if( path.startsWith('flyteidl2.actions.ActionsService/') && !headers['x-actions-project'] )
                return reply(ex, 400, [code: 'invalid_argument', message: 'missing required header: x-actions-project'])
            if( failNext > 0 ) {
                failNext--
                return reply(ex, 503, [code: 'unavailable'])
            }
            if( badRequest )
                return reply(ex, 400, [code: 'invalid_argument', message: 'bad'])
            if( alreadyExists )
                return reply(ex, 409, [code: 'already_exists', message: 'action already exists'])

            final json = requests[path].last().body as Map
            switch( path ) {
                case 'flyteidl2.workflow.RunService/GetActionDetails':
                    final d = details[json.actionId.name]
                    return d ? reply(ex, 200, [details: d]) : reply(ex, 404, [code: 'not_found'])
                case 'flyteidl2.workflow.RunService/CreateRun':
                    return reply(ex, 200, [run: createdRun])
                case 'flyteidl2.workflow.RunService/GetActionDataURIs':
                    return reply(ex, 200, dataUris)
                case 'flyteidl2.workflow.RunService/ListActions':
                    final i = json.request.token ? actionPages.findIndexOf { it.token == json.request.token } + 1 : 0
                    return reply(ex, 200, actionPages[i] ?: [:])
                default:
                    return reply(ex, 200, [:])
            }
        }

        private static Object reply(HttpExchange ex, int code, Map body) {
            final bytes = JsonOutput.toJson(body).getBytes(StandardCharsets.UTF_8)
            ex.responseHeaders.add('Content-Type', 'application/json')
            ex.sendResponseHeaders(code, bytes.length)
            ex.responseBody.withStream { it.write(bytes) }
            return null
        }
    }
}
