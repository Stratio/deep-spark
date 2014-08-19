package com.stratio.deep.rdd;

import scala.runtime.AbstractFunction0;

/**
 * Helper callback class called by Spark when the current RDD is computed successfully. This class
 * simply closes the {@link org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader} passed as an
 * argument.
 *
 * @param <R>
 * @author Luca Rosellini <luca@strat.io>
 */
public class OnComputedRDDCallback<R> extends AbstractFunction0<R> {
    private final IDeepRecordReader recordReader;

    public OnComputedRDDCallback(IDeepRecordReader recordReader) {
        super();
        this.recordReader = recordReader;
    }

    @Override
    public R apply() {
        recordReader.close();

        return null;
    }

}

