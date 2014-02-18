package com.stratio.deep.context;

import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.config.impl.CellDeepJobConfig;
import com.stratio.deep.config.impl.EntityDeepJobConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.rdd.CassandraCellRDD;
import com.stratio.deep.rdd.CassandraGenericRDD;
import com.stratio.deep.rdd.CassandraJavaRDD;
import com.stratio.deep.rdd.CassandraRDD;

/**
 * Entry point to the Cassandra-aware Spark context.
 * 
 * @author Luca Rosellini <luca@strat.io>
 *
 */
public class DeepSparkContext extends JavaSparkContext {

    /**
     * Overridden superclass constructor.
     * 
     * @param sc
     */
    public DeepSparkContext(SparkContext sc) {
	super(sc);
    }

    /**
     * Overridden superclass constructor.
     * 
     * @param master
     * @param appName
     */
    public DeepSparkContext(String master, String appName) {
	super(master, appName);
    }

    /**
     * Overridden superclass constructor.
     * 
     * @param master
     * @param appName
     * @param sparkHome
     * @param jarFile
     */
    public DeepSparkContext(String master, String appName, String sparkHome, String jarFile) {
	super(master, appName, sparkHome, jarFile);
    }

    /**
     * Overridden superclass constructor.
     * 
     * @param master
     * @param appName
     * @param sparkHome
     * @param jars
     */
    public DeepSparkContext(String master, String appName, String sparkHome, String[] jars) {
	super(master, appName, sparkHome, jars);
    }

    /**
     * Overridden superclass constructor.
     * 
     * @param master
     * @param appName
     * @param sparkHome
     * @param jars
     * @param environment
     */
    public DeepSparkContext(String master, String appName, String sparkHome, String[] jars,
	    Map<String, String> environment) {
	super(master, appName, sparkHome, jars, environment);
    }

    /**
     * Builds a new CassandraRDD.
     *
     * @param config
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T> CassandraJavaRDD<T> cassandraJavaRDD(IDeepJobConfig<T> config) {
	if (config instanceof EntityDeepJobConfig) {
	    return new CassandraJavaRDD<T>(cassandraEntityRDD((EntityDeepJobConfig) config));
	}

	if (config instanceof CellDeepJobConfig) {
	    return new CassandraJavaRDD<T>((CassandraGenericRDD<T>) cassandraGenericRDD((CellDeepJobConfig) config));
	}

	throw new DeepGenericException("not recognized config type");
    }

    /**
    * Builds a new entity based CassandraRDD.
    *
    * @param config
    * @return
    */
    public <T extends IDeepType> CassandraGenericRDD<T> cassandraEntityRDD(IDeepJobConfig<T> config) {
	return new CassandraRDD<T>(sc(), config);
    }

    /**
     * Builds a new generic (cell based) CassandraRDD.
     *
     * @param config
     * @return
     */
    public CassandraGenericRDD<Cells> cassandraGenericRDD(IDeepJobConfig<Cells> config) {
	return new CassandraCellRDD(sc(), config);
    }
}
