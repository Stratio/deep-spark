package com.stratio.deep.functions;

import java.io.Serializable;

import scala.runtime.AbstractFunction1;

/**
 * Created by luca on 20/01/14.
 */
public abstract class AbstractSerializableFunction1<T, U> extends AbstractFunction1<T, U> implements Serializable {

    private static final long serialVersionUID = 3528398361663219123L;
}
