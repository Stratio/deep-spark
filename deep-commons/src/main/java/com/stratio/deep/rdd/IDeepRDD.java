package com.stratio.deep.rdd;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.utils.Pair;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobID;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by rcrespo on 4/08/14.
 */
public interface IDeepRDD<T> extends Serializable {


    Iterator<T> compute(Partition split, TaskContext ctx, Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?,?>>> config);

    Partition[] getPartitions(Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?,?>>> config, int id);

//    Partition[] getPartitions(Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?,?>>> config, JobID id );

    /**
     * Transform a row coming from the Cassandra's API to an element of
     * type <T>.
     *
     * @param elem the element to transform.
     * @return the transformed element.
     */
    T transformElement(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem, Broadcast<IDeepJobConfig<T, ? extends IDeepJobConfig<?,?>>> config);
}

