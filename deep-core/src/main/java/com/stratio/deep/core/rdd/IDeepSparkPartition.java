package com.stratio.deep.core.rdd;

import com.stratio.deep.rdd.IDeepPartition;
import org.apache.spark.Partition;

/**
 * Created by rcrespo on 18/08/14.
 */
public interface IDeepSparkPartition extends IDeepPartition, Partition {


}
