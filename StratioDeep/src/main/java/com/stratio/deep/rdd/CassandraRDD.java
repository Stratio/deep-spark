package com.stratio.deep.rdd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.config.impl.GenericDeepJobConfig;
import com.stratio.deep.cql.DeepCqlPagingInputFormat;
import com.stratio.deep.cql.DeepCqlPagingRecordReader;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.functions.CellList2TupleFunction;
import com.stratio.deep.functions.DeepType2TupleFunction;
import com.stratio.deep.partition.impl.DeepPartition;
import org.apache.cassandra.hadoop.cql3.DeepCqlOutputFormat;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import static scala.collection.JavaConversions.asScalaBuffer;
import static scala.collection.JavaConversions.asScalaIterator;

/**
 * Base class that abstracts the complexity of interacting with the Cassandra Datastore.<br/>
 * Implementors should only provide a way to convert an object of type T to a {@link com.stratio.deep.entity.Cells} element.
 */
public abstract class CassandraRDD<T> extends RDD<T> {

    private static final long serialVersionUID = -7338324965474684418L;
    protected static final String STRATIO_DEEP_JOB_PREFIX = "stratio-deep-job-";
    protected static final String STRATIO_DEEP_TASK_PREFIX = "stratio-deep-task-";

    /*
     * An Hadoop Job Id is needed by the underlying cassandra's API.
     *

     * We make it transient in order to prevent this to be sent through the wire
     * to slaves.
     */
    protected final transient JobID hadoopJobId;

    /*
     * RDD configuration. This config is broadcasted to all the Sparks machines.
     */
    protected final Broadcast<IDeepJobConfig<T>> config;

    /**
     * Transform a row coming from the Cassandra's API to an element of
     * type <T>.
     *
     * @param elem
     * @return
     */
    protected abstract T transformElement(Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> elem);

    /**
     * Helper callback class called by Spark when the current RDD is computed
     * successfully. This class simply closes the {@link org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader}
     * passed as an argument.
     *
     * @param <R>
     * @author Luca Rosellini <luca@strat.io>
     */
    class OnComputedRDDCallback<R> extends AbstractFunction0<R> {
	private final org.apache.hadoop.mapred.RecordReader<Map<String, ByteBuffer>, Map<String, ByteBuffer>> recordReader;
	private final DeepPartition deepPartition;

	public OnComputedRDDCallback(
			org.apache.hadoop.mapred.RecordReader<Map<String, ByteBuffer>, Map<String, ByteBuffer>> recordReader,
			DeepPartition dp) {
	    super();
	    this.recordReader = recordReader;
	    this.deepPartition = dp;
	}

	@Override
	public R apply() {
	    try {
		log().debug("Closing context for partition " + deepPartition);

		recordReader.close();
	    } catch (IOException e) {
		throw new DeepIOException(e);
	    }
	    return null;
	}

    }

    /**
     * Provided the mapping function <i>transformer</i> that transforms a generic RDD to an RDD<Tuple2<Cells, Cells>>, 
     * this generic method persists the RDD to underlying Cassandra datastore.
     *
     * @param rdd
     * @param writeConfig
     * @param transformer
     */
    private static <W> void doSaveToCassandra(RDD<W> rdd, IDeepJobConfig<W> writeConfig,
		    Function1<W, Tuple2<Cells, Cells>> transformer) {

	Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> tuple = new Tuple2<>(null, null);

	RDD<Tuple2<Cells, Cells>> mappedRDD = rdd.map(transformer,
			ClassTag$.MODULE$.<Tuple2<Cells, Cells>>apply(tuple.getClass()));

	ClassTag<Cells> keyClassTag = ClassTag$.MODULE$.apply(Cells.class);

	JavaPairRDD<Cells, Cells> pairRDD = new JavaPairRDD<>(mappedRDD, keyClassTag, keyClassTag);

	pairRDD.saveAsNewAPIHadoopFile(writeConfig.getKeyspace(), Cells.class, Cells.class, DeepCqlOutputFormat.class,
			writeConfig.getConfiguration());
    }

    /**
     * Persists the given RDD of Cells to the underlying Cassandra datastore, using configuration
     * options provided by <i>writeConfig</i>.
     *
     * @param rdd
     * @param writeConfig
     */
    public static <W, T extends IDeepType> void saveRDDToCassandra(RDD<W> rdd, IDeepJobConfig<W> writeConfig) {
	if (IDeepType.class.isAssignableFrom(writeConfig.getEntityClass())) {
	    IDeepJobConfig<T> c = (IDeepJobConfig<T>) writeConfig;
	    RDD<T> r = (RDD<T>) rdd;

	    doSaveToCassandra(r, c, new DeepType2TupleFunction<T>());
	} else if (Cells.class.isAssignableFrom(writeConfig.getEntityClass())) {
	    IDeepJobConfig<Cells> c = (IDeepJobConfig<Cells>) writeConfig;
	    RDD<Cells> r = (RDD<Cells>) rdd;

	    doSaveToCassandra(r, c, new CellList2TupleFunction());
	} else {
	    throw new IllegalArgumentException("Provided RDD must be an RDD of com.stratio.deep.entity.Cells or an RDD of com.stratio.deep.entity.IDeepType");
	}
    }

    /**
     * Persists the given JavaRDD to the underlying Cassandra datastore.
     *
     * @param rdd
     * @param writeConfig
     * @param <W>
     */
    public static <W> void saveRDDToCassandra(JavaRDD<W> rdd, IDeepJobConfig<W> writeConfig) {
	saveRDDToCassandra(rdd.rdd(), writeConfig);
    }

    @SuppressWarnings("unchecked")
    public CassandraRDD(SparkContext sc, IDeepJobConfig<T> config) {

	super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(config.getEntityClass()));

	long timestamp = System.currentTimeMillis();
	hadoopJobId = new JobID(STRATIO_DEEP_JOB_PREFIX + timestamp, id());
	this.config = sc.broadcast(config);
    }

    /**
     * Computes the current RDD over the given data partition. Returns an
     * iterator of Scala tuples.
     */
    @Override
    public Iterator<T> compute(Partition split, TaskContext ctx) {

	final DeepCqlPagingInputFormat inputFormat = new DeepCqlPagingInputFormat();
	DeepPartition deepPartition = (DeepPartition) split;

	log().debug("Executing compute for split: " + deepPartition);

	final DeepCqlPagingRecordReader recordReader = initRecordReader(ctx, inputFormat, deepPartition);

	/*
	 * Creates a new anonymous iterator inner class and returns it as a
	 * scala iterator.
	 */
	java.util.Iterator<T> recordReaderIterator = new java.util.Iterator<T>() {

	    @Override
	    public boolean hasNext() {
		return recordReader.hasNext();
	    }

	    @Override
	    public T next() {
		return transformElement(recordReader.next());
	    }

	    @Override
	    public void remove() {
		throw new DeepIOException("Method not implemented (and won't be implemented anytime soon!!!)");
	    }
	};

	return asScalaIterator(recordReaderIterator);
    }

    protected AbstractFunction0<BoxedUnit> getComputeCallback(DeepCqlPagingRecordReader recordReader,
		    DeepPartition dp) {
	return new OnComputedRDDCallback<>(recordReader, dp);
    }

    /**
     * Returns the partitions on which this RDD depends on.
     * <p/>
     * Uses the underlying CqlPagingInputFormat in order to retreive the splits.
     * <p/>
     * The number of splits, and hence the number of partitions equals to the
     * number of tokens configured in cassandra.yaml + 1.
     */
    @Override
    public Partition[] getPartitions() {
	final JobContext hadoopJobContext = new JobContext(config.value().getConfiguration(), hadoopJobId);

	final DeepCqlPagingInputFormat cqlInputFormat = new DeepCqlPagingInputFormat();

	List<InputSplit> underlyingInputSplits;
	try {
	    underlyingInputSplits = cqlInputFormat.getSplits(hadoopJobContext);
	} catch (IOException e) {
	    throw new DeepIOException(e);
	}

	Partition[] partitions = new DeepPartition[underlyingInputSplits.size()];

	for (int i = 0; i < underlyingInputSplits.size(); i++) {
	    InputSplit split = underlyingInputSplits.get(i);
	    partitions[i] = new DeepPartition(id(), i, (Writable) split);

	    log().debug("Detected partition: " + partitions[i]);
	}

	return partitions;
    }

    /**
     * Returns a list of hosts on which the given split resides.
     * <p/>
     * TODO: check what happens in an environment where the split is replicated
     * on N machines. It would be optimum if the RDD were computed only on the
     * machine(s) where the split resides.
     */
    @Override
    public Seq<String> getPreferredLocations(Partition split) {
	DeepPartition p = (DeepPartition) split;

	String[] locations = p.splitWrapper().value().getLocations();
	log().debug("getPreferredLocations: " + p);

	return asScalaBuffer(Arrays.asList(locations));
    }

    /**
     * Initializes a {@link org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader} using Cassandra's Hadoop API.
     * <p/>
     * 1. Constructs a {@link org.apache.hadoop.mapreduce.TaskAttemptID}
     * 2. Constructs a {@link org.apache.hadoop.mapreduce.TaskAttemptContext} using the newly constructed
     * {@link org.apache.hadoop.mapreduce.TaskAttemptID} and the hadoop configuration contained
     * inside this RDD configuration object.
     * 3. Creates a new {@link com.stratio.deep.cql.DeepCqlPagingRecordReader}.
     * 4. Initialized the newly created {@link com.stratio.deep.cql.DeepCqlPagingRecordReader}.
     *
     * @param ctx
     * @param inputFormat
     * @param dp
     * @return
     */
    protected DeepCqlPagingRecordReader initRecordReader(TaskContext ctx,
		    final DeepCqlPagingInputFormat inputFormat, final DeepPartition dp) {
	try {

	    TaskAttemptID attemptId = new TaskAttemptID(STRATIO_DEEP_TASK_PREFIX + System.currentTimeMillis(), id(),
			    true, dp.index(), 0);

	    TaskAttemptContext taskCtx = new TaskAttemptContext(config.value().getConfiguration(), attemptId);

	    final DeepCqlPagingRecordReader recordReader = (DeepCqlPagingRecordReader) inputFormat
			    .createRecordReader(dp.splitWrapper().value(), taskCtx);
	    recordReader.setAdditionalFilters( ((GenericDeepJobConfig) config.value()).getAdditionalFilters() );

	    log().debug("Initializing recordReader for split: " + dp);
	    recordReader.initialize(dp.splitWrapper().value(), taskCtx);

	    ctx.addOnCompleteCallback(getComputeCallback(recordReader, dp));

	    return recordReader;
	} catch (IOException | InterruptedException e) {
	    throw new DeepIOException(e);
	}
    }

    /**
     * Adds a new filter on the specified. This will affect the generation of queries<br/>
     * against the underlying Cassandra datastore.
     *
     * @param fieldName
     * @param value
     * @return
     */
    public CassandraRDD<T> filterByField(String fieldName, String value){
	((GenericDeepJobConfig)config.value()).addFilter(fieldName, value);
	return this;
    }

    /**
     * Removes the specified filter on the provided field (if exists).
     *
     * @param fieldName
     * @return
     */
    public CassandraRDD<T> removeFilterOnField(String fieldName){
	((GenericDeepJobConfig)config.value()).removeFilter(fieldName);
	return this;
    }
}
