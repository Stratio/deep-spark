package com.stratio.deep.functions;

import java.io.Serializable;

import scala.runtime.AbstractFunction1;

/**
 * Abstract base type to create serializable functions of tipe 1.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public abstract class AbstractSerializableFunction<T, U> extends AbstractFunction1<T, U> implements Serializable {

    private static final long serialVersionUID = 3528398361663219123L;
}
