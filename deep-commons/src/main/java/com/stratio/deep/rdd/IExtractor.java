package com.stratio.deep.rdd;

import com.stratio.deep.config.ExtractorConfig;

import java.io.Serializable;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IExtractor<T> extends Serializable {


    DeepTokenRange[] getPartitions(ExtractorConfig<T> config);


    boolean hasNext();

    T next();

    void close();

    void initIterator(DeepTokenRange dp,ExtractorConfig<T> config);


//    void save(Cells rawKey, Cells data);

//    Class getConfigClass();
}
