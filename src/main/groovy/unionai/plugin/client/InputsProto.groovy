package unionai.plugin.client

import flyteidl2.core.Literals.Blob
import flyteidl2.core.Literals.BlobMetadata
import flyteidl2.core.Literals.Literal
import flyteidl2.core.Literals.Scalar
import flyteidl2.core.Types.BlobType
import flyteidl2.task.Common.Inputs
import flyteidl2.task.Common.NamedLiteral
import flyteidl2.task.Common.Outputs
import groovy.transform.CompileStatic

/**
 * The `flyteidl2.task.Inputs` of a task action, made only of File / Dir (blob) literals, and the
 * blob URIs in its `flyteidl2.task.Outputs`.
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
        final result = Inputs.newBuilder()
        for( BlobInput it : inputs )
            result.addLiterals(namedLiteral(it))
        return result.build().toByteArray()
    }

    private static NamedLiteral namedLiteral(BlobInput input) {
        final type = BlobType.newBuilder()
            .setDimensionality(input.dir ? BlobType.BlobDimensionality.MULTIPART : BlobType.BlobDimensionality.SINGLE)
        final blob = Blob.newBuilder()
            .setMetadata(BlobMetadata.newBuilder().setType(type))
            .setUri(input.uri)
        return NamedLiteral.newBuilder()
            .setName(input.name)
            .setValue(Literal.newBuilder().setScalar(Scalar.newBuilder().setBlob(blob)))
            .build()
    }

    /**
     * @return the URI of the named File / Dir output, or null when there is none
     */
    static String blobUri(Outputs outputs, String name) {
        for( NamedLiteral it : outputs?.literalsList ?: Collections.<NamedLiteral>emptyList() ) {
            if( it.name == name && it.value.scalar.hasBlob() )
                return it.value.scalar.blob.uri
        }
        return null
    }
}
