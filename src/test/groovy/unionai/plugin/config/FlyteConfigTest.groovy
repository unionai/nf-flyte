package unionai.plugin.config

import nextflow.util.Duration
import spock.lang.Specification

class FlyteConfigTest extends Specification {

    static final Map POD_ENV = [
        _U_EP_OVERRIDE: 'dns:///tenant.hosted.unionai.cloud',
        _U_ORG_NAME: 'acme',
        FLYTE_INTERNAL_EXECUTION_PROJECT: 'proj',
        FLYTE_INTERNAL_EXECUTION_DOMAIN: 'development',
        NF_FLYTE_RUN_NAME: 'r123',
        _U_RUN_BASE: 's3://bucket/metadata/v2/acme/proj/development/r123',
        _UNION_EAGER_API_KEY: 'a2V5',
    ]

    def 'should default everything from the a0 pod env'() {
        when:
        def cfg = new FlyteConfig([:], POD_ENV)

        then:
        cfg.endpoint == 'tenant.hosted.unionai.cloud'
        cfg.org == 'acme'
        cfg.project == 'proj'
        cfg.domain == 'development'
        cfg.runName == 'r123'
        cfg.parentAction == 'a0'
        cfg.runOutputBase == 's3://bucket/metadata/v2/acme/proj/development/r123'
        cfg.apiKey == 'a2V5'
        cfg.pollInterval == Duration.of('5s')
        cfg.queue == null
        cfg.cache
        cfg.missing() == []
    }

    def 'config file should override the env'() {
        when:
        def cfg = new FlyteConfig([endpoint: 'https://other.example.com/', project: 'p2', parentAction: 'a1', queue: 'gpu', pollInterval: '2s', cache: false], POD_ENV)

        then:
        cfg.endpoint == 'other.example.com'
        cfg.project == 'p2'
        cfg.parentAction == 'a1'
        cfg.queue == 'gpu'
        cfg.pollInterval == Duration.of('2s')
        !cfg.cache
    }

    def 'should report missing settings'() {
        when:
        def cfg = new FlyteConfig([:], [:])

        then: 'launched outside Flyte, the executor creates the run, so no run settings are needed'
        cfg.standalone
        cfg.missing() == ['endpoint', 'org', 'project', 'domain', 'apiKey (env FLYTE_API_KEY) or authCommand']

        when: 'inside a Flyte task'
        cfg = new FlyteConfig([:], [NF_FLYTE_RUN_NAME: 'r1', FLYTE_API_KEY: 'k'])

        then:
        !cfg.standalone
        cfg.missing().contains('runOutputBase')
    }

    def 'should take the endpoint and org from the API key'() {
        given:
        def key = Base64.encoder.encodeToString('dns:///tenant.example.com:id:secret:acme'.bytes)

        when:
        def cfg = new FlyteConfig([project: 'p', domain: 'd'], [FLYTE_API_KEY: key])

        then:
        cfg.endpoint == 'tenant.example.com'
        cfg.org == 'acme'
        cfg.missing() == []
    }

    def 'should accept an auth command instead of an API key'() {
        when:
        def cfg = new FlyteConfig([endpoint: 'e', org: 'o', project: 'p', domain: 'd', authCommand: 'my-cli token'], [:])

        then:
        cfg.missing() == []
        !cfg.toString().contains('my-cli')
        cfg.heartbeatTimeout.toSeconds() == 300
        cfg.rootImage == FlyteConfig.DEFAULT_ROOT_IMAGE
    }

    def 'should not print the api key'() {
        expect:
        !new FlyteConfig([:], POD_ENV).toString().contains('a2V5')
    }
}
