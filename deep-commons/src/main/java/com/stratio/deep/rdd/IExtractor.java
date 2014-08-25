package com.stratio.deep.rdd;

import com.stratio.deep.config.ExtractorConfig;
import org.apache.spark.Partition;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IExtractor<T> extends Serializable {


    Partition[] getPartitions(ExtractorConfig<T> config);

    boolean hasNext();

    T next();

    void close();

    void initIterator(Partition dp, ExtractorConfig<T> config);


    IExtractor<T> getExtractorInstance(ExtractorConfig<T> config);

    void saveRDD(RDD<T> rdd, ExtractorConfig<T> config);

}
