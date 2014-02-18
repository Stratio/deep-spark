package com.stratio.deep.rdd;

import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import scala.collection.Iterator;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.IDeepType;

public class CassandraSSTableRDD<T extends IDeepType> extends RDD<T> {

	private static final long serialVersionUID = -6276303969653122857L;
	
	public CassandraSSTableRDD(SparkContext sc, IDeepJobConfig<T> config) {
		
		super(sc, scala.collection.immutable.Seq$.MODULE$.empty(), ClassManifest$.MODULE$.fromClass(config.getEntityClass()) );
		
//		long timestamp = System.currentTimeMillis();
//		hadoopJobId = new JobID(STRATIO_DEEP_JOB_PREFIX + timestamp, id());
//		this.config = sc.broadcast(config);
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.spark.rdd.RDD#compute(org.apache.spark.Partition, org.apache.spark.TaskContext)
	 */
	@Override
	public Iterator<T> compute(Partition arg0, TaskContext arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.spark.rdd.RDD#getPartitions()
	 */
	@Override
	public Partition[] getPartitions() {
		// TODO Auto-generated method stub
		return null;
	}

}
