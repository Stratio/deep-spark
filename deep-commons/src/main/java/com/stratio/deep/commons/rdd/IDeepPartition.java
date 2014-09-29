package com.stratio.deep.commons.rdd;

import org.apache.spark.Partition;

/**
 * Created by rcrespo on 18/08/14.
 */
public interface IDeepPartition extends Partition {

    DeepTokenRange splitWrapper();
}
