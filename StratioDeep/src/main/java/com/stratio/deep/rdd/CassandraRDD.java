package com.stratio.deep.rdd;

import static com.stratio.deep.util.CassandraRDDUtils.*;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.utils.Pair;
import org.apache.spark.SparkContext;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.serializer.IDeepSerializer;

/**
 * Stratio's implementation of an RDD reading and writing data from and to
 * Apache Cassandra. This implementation uses Cassandra's Hadoop API.
 * <p/>
 * We do not use Map<String,ByteBuffer> as key and value objects, since
 * ByteBuffer is not serializable.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public final class CassandraRDD<T extends IDeepType> extends CassandraGenericRDD<T> {

    private static final long serialVersionUID = -3208994171892747470L;

    public CassandraRDD(SparkContext sc, IDeepJobConfig<T> config) {
	super(sc, config);
    }

    @Override
    protected T transformElement(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem) {
	/*
	 * I Need to convert a pair of Map<String, ByteBuffer> to a
	 * tuple of Map<String, DeepByteBuffer<?>>. I need to serialize
	 * each field and then build a DeepByteBuffer with it.
	 */
	Class<T> entityClass = config.value().getEntityClass();
	IDeepSerializer<T> serializer = config.value().getSerializer();
	return pair2DeepType(elem, entityClass, serializer);
    }
}
