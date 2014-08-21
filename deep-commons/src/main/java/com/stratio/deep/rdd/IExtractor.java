package com.stratio.deep.rdd;

import com.stratio.deep.config.DeepJobConfig;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IExtractor<T> extends Serializable {


    DeepTokenRange[] getPartitions(DeepJobConfig<T> config);


    boolean hasNext();

    T next();

    void close();

    void initIterator(final DeepTokenRange dp,
                      DeepJobConfig<T> config);

    // TODO Implement and document
    // void write(Cells rawKey, Cells data);
}
