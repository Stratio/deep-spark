package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.querybuilder.UpdateQueryBuilder;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.jdbc.reader.JdbcReader;
import com.stratio.deep.jdbc.writer.JdbcWriter;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.JdbcRDD;
import scala.Function0;
import scala.Function1;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

/**
 * Created by mariomgal on 09/12/14.
 */
public abstract class JdbcNativeExtractor<T, S extends BaseConfig<T>> extends JdbcRDD<T> implements IExtractor<T, S> {

    private static final long serialVersionUID = -298383130965427783L;

    public JdbcNativeExtractor(SparkContext sc, Function0<Connection> getConnection, String sql, long lowerBound, long upperBound, int numPartitions, Function1<ResultSet, T> mapRow, ClassTag<T> evidence$1) {
        super(sc, getConnection, sql, lowerBound, upperBound, numPartitions, mapRow, evidence$1);
    }

    @Override
    public Partition[] getPartitions(S config) {
        return super.getPartitions();
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void initIterator(Partition dp, S config) {

    }

    @Override
    public void saveRDD(T t) {

    }

    @Override
    public List<String> getPreferredLocations(Partition split) {
        return null;
    }

    @Override
    public void initSave(S config, T first, UpdateQueryBuilder queryBuilder) {

    }
}
