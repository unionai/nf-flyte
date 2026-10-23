package unionai.plugin.executor

import nextflow.Session
import nextflow.config.Manifest
import nextflow.script.WorkflowMetadata
import spock.lang.Specification

class FlyteExecutorTest extends Specification {

    def 'should name the pipeline after its manifest, project or main script'() {
        given:
        def meta = Stub(WorkflowMetadata) { getProjectName() >> project }
        def session = Stub(Session) {
            getManifest() >> new Manifest(manifest ? [name: manifest] : [:])
            getWorkflowMetadata() >> meta
            getScriptName() >> script
        }

        expect:
        FlyteExecutor.pipelineName(session) == expected

        where:
        manifest         | project          | script     | expected
        'nf-core/rnaseq' | 'nf-core/rnaseq' | 'main.nf'  | 'nf-core/rnaseq'
        null             | 'acme/pipeline'  | 'main.nf'  | 'acme/pipeline'
        null             | null             | 'hello.nf' | 'hello'
        null             | null             | null       | null
    }
}
