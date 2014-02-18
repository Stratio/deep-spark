package com.stratio.deep.rdd;

import java.nio.ByteBuffer;
import java.util.Map;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import org.apache.cassandra.utils.Pair;
import org.apache.spark.SparkContext;

/**
 * Created by luca on 04/02/14.
 */
public class CassandraCellRDD extends CassandraGenericRDD<Cells> {

    private static final long serialVersionUID = -738528971629963221L;

    public CassandraCellRDD(SparkContext sc, IDeepJobConfig<Cells> config) {
	super(sc, config);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Cells transformElement(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem) {

	Cells cells = new Cells();
	Map<String, Cell> columnDefinitions = config.value().columnDefinitions();

	for (Map.Entry<String, ByteBuffer> entry : elem.left.entrySet()) {
	    Cell cd = columnDefinitions.get(entry.getKey());
	    cells.add(Cell.create(entry.getKey(), entry.getValue(), cd.marshallerClassName(), cd.isPartitionKey(),
		    cd.isClusterKey()));
	}

	for (Map.Entry<String, ByteBuffer> entry : elem.right.entrySet()) {
	    Cell cd = columnDefinitions.get(entry.getKey());
	    cells.add(Cell.create(entry.getKey(), entry.getValue(), cd.marshallerClassName(), cd.isPartitionKey(),
			    cd.isClusterKey()));
	}

	return cells;
    }
}
