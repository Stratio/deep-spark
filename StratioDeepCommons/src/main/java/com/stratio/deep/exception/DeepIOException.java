package com.stratio.deep.exception;

import java.io.IOException;

/**
 * Unchecked variant of {@link IOException}.
 * 
 * @author Luca Rosellini <luca@stratio.com>
 *
 */
public class DeepIOException extends RuntimeException {

    private static final long serialVersionUID = 6496798210905077525L;

    public DeepIOException() {
	super();
    }

    public DeepIOException(String msg) {
	super(msg);
    }

    public DeepIOException(String message, Throwable cause) {
	super(message, cause);
    }

    public DeepIOException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
	super(message, cause, enableSuppression, writableStackTrace);

    }

    public DeepIOException(Throwable cause) {
	super(cause);
    }

}
