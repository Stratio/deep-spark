package org.apache.cassandra.hadoop.cql3;

import static com.stratio.deep.util.CassandraRDDUtils.*;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.hadoop.AbstractColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.AbstractColumnFamilyRecordWriter;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.Progressable;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.exception.DeepGenericException;

/**
 * Created by luca on 05/02/14.
 */
public class DeepCqlRecordWriter extends AbstractColumnFamilyRecordWriter<Cells, Cells> {
    /**
     * A client that runs in a threadpool and connects to the list of endpoints for a particular
     * range. Bound variables for keys in that range are sent to this client via a queue.
     */
    public class RangeClient extends AbstractRangeClient<List<ByteBuffer>> implements Closeable {
	/**
	 * Constructs an {@link RangeClient} for the given endpoints.
	 *
	 * @param endpoints the possible endpoints to execute the mutations on
	 */
	public RangeClient(List<InetAddress> endpoints) {
	    super(endpoints);
	}

	/**
	 * get prepared statement id from cache, otherwise prepare it from Cassandra server
	 */
	private Integer preparedStatement(Cassandra.Client client) {
	    Integer itemId = preparedStatements.get(client);
	    if (itemId == null) {
		CqlPreparedResult result;
		try {
		    result = client.prepare_cql3_query(ByteBufferUtil.bytes(cql), Compression.NONE);
		} catch (TException e) {
		    throw new DeepGenericException("failed to prepare cql query " + cql, e);
		}

		Integer previousId = preparedStatements.putIfAbsent(client, result.itemId);
		itemId = previousId == null ? result.itemId : previousId;
	    }
	    return itemId;
	}

	/**
	 * Loops collecting cql binded variable values from the queue and sending to Cassandra
	 */
	@Override
	public void run() {
	    outer: while (run || !queue.isEmpty()) {
		List<ByteBuffer> bindVariables;
		try {
		    bindVariables = queue.take();
		} catch (InterruptedException e) {
		    // re-check loop condition after interrupt
		    continue;
		}

		Iterator<InetAddress> iter = endpoints.iterator();
		while (true) {
		    // send the mutation to the last-used endpoint.  first time through, this will NPE harmlessly.
		    try {
			int i = 0;
			int itemId = preparedStatement(client);
			while (bindVariables != null) {
			    client.execute_prepared_cql3_query(itemId, bindVariables, ConsistencyLevel.ONE);
			    i++;

			    if (i >= batchThreshold) {
				break;
			    }

			    bindVariables = queue.poll();
			}

			break;
		    } catch (Exception e) {
			closeInternal();
			if (!iter.hasNext()) {
			    lastException = new IOException(e);
			    break outer;
			}
		    }

		    // attempt to connect to a different endpoint
		    try {
			InetAddress address = iter.next();
			String host = address.getHostName();
			int port = ConfigHelper.getOutputRpcPort(conf);
			client = AbstractColumnFamilyOutputFormat.createAuthenticatedClient(host, port, conf);
		    } catch (Exception e) {
			closeInternal();
			// TException means something unexpected went wrong to that endpoint, so
			// we should try again to another.  Other exceptions (auth or invalid request) are fatal.
			if ((!(e instanceof TException)) || !iter.hasNext()) {
			    lastException = new IOException(e);
			    break outer;
			}
		    }
		}
	    }
	}
    }

    private static final Logger logger = LoggerFactory.getLogger(CqlRecordWriter.class);

    // handles for clients for each range running in the threadpool
    protected final Map<Range<?>, RangeClient> clients;

    // host to prepared statement id mappings
    protected ConcurrentHashMap<Cassandra.Client, Integer> preparedStatements = new ConcurrentHashMap<>();
    protected AbstractType<?> keyValidator;
    protected String[] partitionKeyColumns;

    protected List<String> clusterColumns;

    private String cql;

    DeepCqlRecordWriter(Configuration conf) {
	super(conf);
	this.clients = new HashMap<>();
	init(conf);
    }

    DeepCqlRecordWriter(Configuration conf, Progressable progressable) throws IOException {
	this(conf);
	this.progressable = progressable;
    }

    /**
     * Upon construction, obtain the map that this writer will use to collect
     * mutations, and the ring cache for the given keyspace.
     *
     * @param context the task attempt context
     * @throws IOException
     */
    DeepCqlRecordWriter(TaskAttemptContext context) throws IOException {
	this(context.getConfiguration());
	this.progressable = new Progressable(context);
    }

    @Override
    public void close() throws IOException {
	// close all the clients before throwing anything
	for (RangeClient client : clients.values()) {
	    IOUtils.closeQuietly(client);
	}
    }

    protected ByteBuffer getPartitionKey(Cells cells) {
	ByteBuffer partitionKey;
	if (keyValidator instanceof CompositeType) {
	    ByteBuffer[] keys = new ByteBuffer[partitionKeyColumns.length];

	    for (int i = 0; i < cells.size(); i++) {
		Cell<?> c = cells.getCellByIdx(i);

		if (c.isPartitionKey()) {
		    keys[i] = c.getDecomposedCellValue();
		}
	    }

	    partitionKey = CompositeType.build(keys);
	} else {
	    partitionKey = cells.getCellByIdx(0).getDecomposedCellValue();
	}
	return partitionKey;
    }

    protected void init(Configuration conf) {
	try {
	    Cassandra.Client client = ConfigHelper.getClientFromOutputAddressList(conf);
	    retrievePartitionKeyValidator(client);

	    TTransport transport = client.getOutputProtocol().getTransport();
	    if (transport.isOpen()) {
		transport.close();
	    }

	} catch (Exception e) {
	    throw new DeepGenericException(e);
	}
    }

    protected AbstractType<?> parseType(String type) throws ConfigurationException {
	try {
	    // always treat counters like longs, specifically CCT.serialize is not what we need
	    if (type != null && type.equals("org.apache.cassandra.db.marshal.CounterColumnType")) {
		return LongType.instance;
	    }
	    return TypeParser.parse(type);
	} catch (SyntaxException e) {
	    throw new ConfigurationException(e.getMessage(), e);
	}
    }

    /**
     * Quoting for working with uppercase
     */
    protected String quote(String identifier) {
	StringBuilder sb = new StringBuilder("\"").append(identifier.replaceAll("\"", "\"\"")).append("\"");
	return sb.toString();
    }

    /**
     * retrieve the key validator from system.schema_columnfamilies table
     */
    protected void retrievePartitionKeyValidator(Cassandra.Client client) throws Exception {
	String keyspace = ConfigHelper.getOutputKeyspace(conf);
	String cfName = ConfigHelper.getOutputColumnFamily(conf);

	String query = "SELECT key_validator," + "       key_aliases," + "       column_aliases "
		+ "FROM system.schema_columnfamilies " + "WHERE keyspace_name='%s' and columnfamily_name='%s'";
	String formatted = String.format(query, keyspace, cfName);
	CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(formatted), Compression.NONE,
		ConsistencyLevel.ONE);

	Column rawKeyValidator = result.rows.get(0).columns.get(0);
	String validator = ByteBufferUtil.string(ByteBuffer.wrap(rawKeyValidator.getValue()));
	keyValidator = parseType(validator);

	Column rawPartitionKeys = result.rows.get(0).columns.get(1);
	String keyString = ByteBufferUtil.string(ByteBuffer.wrap(rawPartitionKeys.getValue()));
	logger.debug("partition keys: " + keyString);

	List<String> keys = FBUtilities.fromJsonList(keyString);
	partitionKeyColumns = new String[keys.size()];
	int i = 0;
	for (String key : keys) {
	    partitionKeyColumns[i] = key;
	    i++;
	}

	Column rawClusterColumns = result.rows.get(0).columns.get(2);
	String clusterColumnString = ByteBufferUtil.string(ByteBuffer.wrap(rawClusterColumns.getValue()));

	logger.debug("cluster columns: " + clusterColumnString);
	clusterColumns = FBUtilities.fromJsonList(clusterColumnString);
    }

    /**
     * If the key is to be associated with a valid value, a mutation is created
     * for it with the given column family and columns. In the event the value
     * in the column is missing (i.e., null), then it is marked for
     * {@link org.apache.cassandra.thrift.Deletion}. Similarly, if the entire value for a key is missing
     * (i.e., null), then the entire key is marked for {@link org.apache.cassandra.thrift.Deletion}.
     * </p>
     *
     * @throws IOException
     */
    @Override
    public void write(Cells keys, Cells values) throws IOException {
	/* generate SQL */
	cql = updateQueryGenerator(keys, values, ConfigHelper.getOutputKeyspace(conf),
		ConfigHelper.getOutputColumnFamily(conf));

	@SuppressWarnings("rawtypes")
	Range<Token> range = ringCache.getRange(getPartitionKey(keys));

	// get the client for the given range, or create a new one
	RangeClient client = clients.get(range);
	if (client == null) {
	    // haven't seen keys for this range: create new client
	    client = new RangeClient(ringCache.getEndpoint(range));
	    client.start();
	    clients.put(range, client);
	}

	// add primary key columns to the bind variables
	List<ByteBuffer> allValues = new ArrayList<>(values.getDecomposedCellValues());
	allValues.addAll(keys.getDecomposedCellValues());
	client.put(allValues);
	progressable.progress();
    }
}
