package com.stratio.deep.exception;

/**
 * Unchecked variant of {@link NoSuchFieldException}.
 * 
 * @author Luca Rosellini <luca@stratio.com>
 *
 */
public class DeepNoSuchFieldException extends RuntimeException {
    private static final long serialVersionUID = 4552228341917254900L;

    public DeepNoSuchFieldException(String message, Throwable cause) {
	super(message, cause);
    }

    public DeepNoSuchFieldException(Throwable cause) {
	super(cause);
    }

    public DeepNoSuchFieldException(String cause) {
	super(cause);
    }

}
