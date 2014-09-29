package com.stratio.deep.commons.rdd;

import java.io.Serializable;

import org.apache.spark.Partition;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.config.IDeepJobConfig;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IExtractor<T> extends Serializable {

    void initIterator(Partition dp,
            IDeepJobConfig<T, ?> deepJobConfig);

    Partition[] getPartitions(ExtractorConfig<T> config);

    Partition[] getPartitions(IDeepJobConfig<T, ?> deepJobConfig);

    boolean hasNext();

    T next();

    void close();

    void initIterator(Partition dp, ExtractorConfig<T> config);

    IExtractor<T> getExtractorInstance(ExtractorConfig<T> config);

    void saveRDD(T t);

    void initSave(ExtractorConfig<T> config, T first);
}
