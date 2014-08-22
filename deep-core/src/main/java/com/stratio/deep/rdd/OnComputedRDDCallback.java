package com.stratio.deep.rdd;

import com.stratio.deep.extractor.client.ExtractorClient;
import scala.runtime.AbstractFunction0;

/**
 * Helper callback class called by Spark when the current RDD is computed successfully. This class
 * simply closes the {@link org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader} passed as an
 * argument.
 *
 * @param <T>
 * @author Luca Rosellini <luca@strat.io>
 */
public class OnComputedRDDCallback<T> extends AbstractFunction0<T> {
    private final IExtractorClient<T> extractorClient;

    public OnComputedRDDCallback(IExtractorClient<T> extractorClient) {
        super();
        this.extractorClient = extractorClient;
    }

    @Override
    public T apply() {
        extractorClient.close();

        return null;
    }

}

