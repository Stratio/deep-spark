package com.stratio.deep.commons.exception;

/**
 * Created by jmgomez on 22/08/14.
 */
public class DeepExtractorinitializationException extends RuntimeException {

    /**
     * Default constructor.
     */
    public DeepExtractorinitializationException() {
        super();
    }

    /**
     * Public constructor.
     */
    public DeepExtractorinitializationException(String message) {
        super(message);
    }

    /**
     * Public constructor.
     */
    public DeepExtractorinitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Public constructor.
     */
    public DeepExtractorinitializationException(Throwable cause) {
        super(cause);
    }
}
