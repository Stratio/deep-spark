package com.stratio.deep.rdd;

import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.exception.DeepExtractorinitializationException;

import java.io.Serializable;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IExtractorClient<T> extends Serializable {


    DeepTokenRange[] getPartitions(ExtractorConfig<T> config);


    boolean hasNext();

    T next();

    void close();

    void initIterator(final DeepTokenRange dp,
                      ExtractorConfig<T> config);

    void initialize() throws DeepExtractorinitializationException;


}
