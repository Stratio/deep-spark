package com.stratio.deep.commons.rdd;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.Partition;

import com.stratio.deep.commons.config.BaseConfig;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IExtractor<T, S extends BaseConfig<T>> extends Serializable {

    Partition[] getPartitions(S config);

    boolean hasNext();

    T next();

    void close();

    void initIterator(Partition dp, S config);

    void saveRDD(T t);

    void saveMaxRDD(T first, String columnName, List<String> primaryKeys);

    void initSave(S config, T first);

	void initSaveMax(S config, T first);
}
