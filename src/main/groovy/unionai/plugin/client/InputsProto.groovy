package unionai.plugin.client

import java.nio.charset.StandardCharsets

import groovy.transform.CompileStatic

/**
 * Binary encoding of the one protobuf message the head has to write itself: the
 * `flyteidl2.task.Inputs` of a task action, made only of File / Dir (blob) literals.
 * Everything else goes over Connect+JSON, so this avoids generated protobuf classes.
 *
 * <pre>
 * Inputs       { repeated NamedLiteral literals = 1; }
 * NamedLiteral { string name = 1; Literal value = 2; }
 * Literal      { Scalar scalar = 1; }
 * Scalar       { Blob blob = 2; }
 * Blob         { BlobMetadata metadata = 1; string uri = 3; }
 * BlobMetadata { BlobType type = 1; }
 * BlobType     { string format = 1; BlobDimensionality dimensionality = 2; }  // SINGLE = 0, MULTIPART = 1
 * </pre>
 */
@CompileStatic
class InputsProto {

    static class BlobInput {
        final String name
        final String uri
        final boolean dir

        BlobInput(String name, String uri, boolean dir) {
            this.name = name
            this.uri = uri
            this.dir = dir
        }
    }

    static byte[] encode(List<BlobInput> inputs) {
        final out = new ByteArrayOutputStream()
        for( BlobInput it : inputs )
            message(out, 1, namedLiteral(it))
        return out.toByteArray()
    }

    private static byte[] namedLiteral(BlobInput input) {
        final blobType = new ByteArrayOutputStream()
        // proto3 leaves out default values: empty format, SINGLE dimensionality
        if( input.dir )
            varintField(blobType, 2, 1)

        final metadata = new ByteArrayOutputStream()
        message(metadata, 1, blobType.toByteArray())

        final blob = new ByteArrayOutputStream()
        message(blob, 1, metadata.toByteArray())
        string(blob, 3, input.uri)

        final scalar = new ByteArrayOutputStream()
        message(scalar, 2, blob.toByteArray())

        final literal = new ByteArrayOutputStream()
        message(literal, 1, scalar.toByteArray())

        final named = new ByteArrayOutputStream()
        string(named, 1, input.name)
        message(named, 2, literal.toByteArray())
        return named.toByteArray()
    }

    private static void string(ByteArrayOutputStream out, int field, String value) {
        if( value )
            message(out, field, value.getBytes(StandardCharsets.UTF_8))
    }

    private static void message(ByteArrayOutputStream out, int field, byte[] bytes) {
        varint(out, (field << 3) | 2)
        varint(out, bytes.length)
        out.write(bytes, 0, bytes.length)
    }

    private static void varintField(ByteArrayOutputStream out, int field, long value) {
        varint(out, field << 3)
        varint(out, value)
    }

    private static void varint(ByteArrayOutputStream out, long value) {
        long v = value
        while( (v & ~0x7FL) != 0 ) {
            out.write((int) ((v & 0x7F) | 0x80))
            v >>>= 7
        }
        out.write((int) v)
    }
}
