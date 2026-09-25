package unionai.plugin.client

import spock.lang.Specification
import unionai.plugin.client.InputsProto.BlobInput

class InputsProtoTest extends Specification {

    def 'should encode exactly like the protobuf runtime'() {
        given: 'bytes from flyteidl2.task.common_pb2.Inputs(...).SerializeToString() in Python'
        def expected = '0a320a04696e5f30122a0a2812260a020a001a2073333a2f2f622f776f726b2f61622f63642f73616d706c655f312e66712e677a0a2c0a066e665f62696e12220a20121e0a040a0210011a1673333a2f2f622f776f726b2f746d702f78792f62696e'

        when:
        def bytes = InputsProto.encode([
            new BlobInput('in_0', 's3://b/work/ab/cd/sample_1.fq.gz', false),
            new BlobInput('nf_bin', 's3://b/work/tmp/xy/bin', true),
        ])

        then:
        bytes.encodeHex().toString() == expected
    }

    def 'should encode no inputs as an empty message'() {
        expect:
        InputsProto.encode([]).length == 0
    }

    def 'should encode lengths over 127 as multi-byte varints'() {
        given:
        def uri = 's3://b/' + 'x' * 300

        when:
        def bytes = InputsProto.encode([new BlobInput('in_0', uri, false)])

        then: 'the outer length is a 2-byte varint and the uri survives intact'
        (bytes[1] & 0x80) != 0
        new String(bytes, 'UTF-8').contains(uri)
    }
}
