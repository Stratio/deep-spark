package com.stratio.deep.commons.rdd;

import java.io.Serializable;

import org.apache.spark.Partition;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.functions.SaveFunction;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IExtractor<T, S extends BaseConfig<T>> extends Serializable {

    Partition[] getPartitions(S config);

    boolean hasNext();

    T next();

    void close();

    void initIterator(Partition dp, S config);

    void saveRDD(T t, SaveFunction function);

    void initSave(S config, T first, SaveFunction function);
}
