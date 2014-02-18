package com.stratio.deep.exception;

/**
 * Generic Deep exception. 
 * 
 * @author Luca Rosellini <luca@stratio.com>
 *
 */
public class DeepGenericException extends RuntimeException {

    private static final long serialVersionUID = -1912500294194290036L;

    /**
     * Default constructor.
     * 
     */
    public DeepGenericException() {
	super();
    }

    public DeepGenericException(String message) {
	super(message);
    }

    public DeepGenericException(String message, Throwable cause) {
	super(message, cause);
    }

    public DeepGenericException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
	super(message, cause, enableSuppression, writableStackTrace);
    }

    public DeepGenericException(Throwable cause) {
	super(cause);
    }

}
