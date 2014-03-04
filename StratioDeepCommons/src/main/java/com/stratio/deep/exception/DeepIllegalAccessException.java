package com.stratio.deep.exception;

/**
 * Unchecked variant of {@link IllegalAccessException}.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public class DeepIllegalAccessException extends RuntimeException {

    private static final long serialVersionUID = -420912657203318307L;

    public DeepIllegalAccessException(String message) {
        super(message);
    }

    public DeepIllegalAccessException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeepIllegalAccessException(Throwable cause) {
        super(cause);
    }

}
