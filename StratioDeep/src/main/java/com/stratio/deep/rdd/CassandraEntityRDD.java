package com.stratio.deep.rdd;

import java.nio.ByteBuffer;
import java.util.Map;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.config.impl.EntityDeepJobConfig;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import com.stratio.deep.util.Utils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Pair;
import org.apache.spark.SparkContext;

/**
 * Stratio's implementation of an RDD reading and writing data from and to
 * Apache Cassandra. This implementation uses Cassandra's Hadoop API.
 * <p/>
 * We do not use Map<String,ByteBuffer> as key and value objects, since
 * ByteBuffer is not serializable.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public final class CassandraEntityRDD<T extends IDeepType> extends CassandraRDD<T> {

    private static final long serialVersionUID = -3208994171892747470L;

    public CassandraEntityRDD(SparkContext sc, IDeepJobConfig<T> config) {
	super(sc, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected T transformElement(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem) {
	Map<String, Cell> columnDefinitions = config.value().columnDefinitions();

	Class<T> entityClass = config.value().getEntityClass();

	EntityDeepJobConfig<T> edjc = (EntityDeepJobConfig) config.value();
	T instance = Utils.newTypeInstance(entityClass);

	for (Map.Entry<String, ByteBuffer> entry : elem.left.entrySet()) {
	    Cell metadata = columnDefinitions.get(entry.getKey());
	    AbstractType<?> marshaller = metadata.marshaller();
	    edjc.setInstancePropertyFromDbName(instance, entry.getKey(), marshaller.compose(entry.getValue()));
	}

	for (Map.Entry<String, ByteBuffer> entry : elem.right.entrySet()) {
	    if (entry.getValue() == null) {
		continue;
	    }

	    Cell metadata = columnDefinitions.get(entry.getKey());
	    AbstractType<?> marshaller = metadata.marshaller();
	    try {
		edjc.setInstancePropertyFromDbName(instance, entry.getKey(), marshaller.compose(entry.getValue()));
	    } catch (DeepNoSuchFieldException e) {
		log().debug(e.getMessage());
	    }
	}

	return instance;
    }
}
