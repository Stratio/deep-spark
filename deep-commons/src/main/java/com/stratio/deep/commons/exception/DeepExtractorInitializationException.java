package com.stratio.deep.commons.exception;

/**
 * Created by jmgomez on 22/08/14.
 */
public class DeepExtractorInitializationException extends RuntimeException {

    /**
     * Default constructor.
     */
    public DeepExtractorInitializationException() {
        super();
    }

    /**
     * Public constructor.
     */
    public DeepExtractorInitializationException(String message) {
        super(message);
    }

    /**
     * Public constructor.
     */
    public DeepExtractorInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Public constructor.
     */
    public DeepExtractorInitializationException(Throwable cause) {
        super(cause);
    }
}
