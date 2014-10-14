package com.stratio.deep.commons.rdd;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import java.io.Serializable;
import org.apache.spark.Partition;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.config.IDeepJobConfig;

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

    void initSave(S config, T first);
}
