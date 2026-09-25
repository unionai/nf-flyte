package unionai.plugin.client

import groovy.transform.CompileStatic

@CompileStatic
class FlyteException extends RuntimeException {

    FlyteException(String message) {
        super(message)
    }

    FlyteException(String message, Throwable cause) {
        super(message, cause)
    }
}
