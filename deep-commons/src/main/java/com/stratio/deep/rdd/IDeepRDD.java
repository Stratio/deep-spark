package com.stratio.deep.rdd;

import com.stratio.deep.config.ExtractorConfig;

import java.io.Serializable;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IDeepRDD<T> extends Serializable {


    DeepTokenRange[] getPartitions(ExtractorConfig<T> config);


    boolean hasNext();

    T next();

    void close();

    void initIterator(final DeepTokenRange dp,
                      ExtractorConfig<T> config);

    // TODO Implement and document
    // void write(Cells rawKey, Cells data);
}
