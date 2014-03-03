package com.stratio.deep.rdd;

import org.apache.spark.api.java.JavaRDD;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * Commodity RDD implementation that should be used as a
 * Java Wrapper for {@link CassandraEntityRDD}.
 *
 * @param <W>
 * @author Luca Rosellini <luca@strat.io>
 */
public final class CassandraJavaRDD<W> extends JavaRDD<W>
{
  private static final long serialVersionUID = -3208994171892747470L;

  /**
   * Default constructor. Constructs a new Java-friendly Cassandra RDD
   *
   * @param rdd
   */
  public CassandraJavaRDD(CassandraRDD<W> rdd)
  {
    super(rdd, ClassTag$.MODULE$.<W>apply(rdd.config.value().getEntityClass()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassTag<W> classTag()
  {
    return ClassTag$.MODULE$.<W>apply(((CassandraRDD<W>) this.rdd()).config.value().getEntityClass());
  }
}
