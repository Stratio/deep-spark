package com.stratio.deep.exception;

/**
 * Created by luca on 28/02/14.
 */
public class DeepIndexNotFoundException extends RuntimeException{

    public DeepIndexNotFoundException(String cause){
	super(cause);
    }
}
