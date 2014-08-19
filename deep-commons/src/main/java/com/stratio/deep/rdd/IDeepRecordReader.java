package com.stratio.deep.rdd;

import java.io.Serializable;

/**
 * Created by rcrespo on 18/08/14.
 */
public interface IDeepRecordReader<T> extends Serializable {

    boolean hasNext();

    T next();

    void close();
}
