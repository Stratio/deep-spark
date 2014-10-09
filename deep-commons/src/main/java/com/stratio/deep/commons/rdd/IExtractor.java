package com.stratio.deep.commons.rdd;

import com.stratio.deep.commons.config.ExtractorConfig;
import org.apache.spark.Partition;

import java.io.Serializable;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IExtractor<T> extends Serializable {


    Partition[] getPartitions(ExtractorConfig<T> config);

//    Partition[] getPartitions(DeepJobConfig<T> config);

    boolean hasNext();

    T next();

    void close();

    void initIterator(Partition dp, ExtractorConfig<T> config);

//    void initIterator(Partition dp, DeepJobConfig<T> config);

    void saveRDD(T t);

    void initSave(ExtractorConfig<T> config, T first);
}
