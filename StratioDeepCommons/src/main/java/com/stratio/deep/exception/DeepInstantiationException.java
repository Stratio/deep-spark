package com.stratio.deep.exception;

/**
 * Created by luca on 04/02/14.
 */
public class DeepInstantiationException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = -8281399377853619766L;

    public DeepInstantiationException() {
	super();
    }

    public DeepInstantiationException(String msg) {
	super(msg);
    }

    public DeepInstantiationException(String message, Throwable cause) {
	super(message, cause);
    }

    public DeepInstantiationException(String message, Throwable cause, boolean enableSuppression,
	    boolean writableStackTrace) {
	super(message, cause, enableSuppression, writableStackTrace);
    }

    public DeepInstantiationException(Throwable cause) {
	super(cause);
    }
}
