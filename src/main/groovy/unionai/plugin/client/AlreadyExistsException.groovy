package unionai.plugin.client

import groovy.transform.CompileStatic

@CompileStatic
class AlreadyExistsException extends FlyteException {

    AlreadyExistsException(String message) {
        super(message)
    }
}
