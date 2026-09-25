package unionai.plugin.client

import groovy.transform.CompileStatic

@CompileStatic
class NotFoundException extends FlyteException {

    NotFoundException(String message) {
        super(message)
    }
}
