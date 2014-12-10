package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.querybuilder.UpdateQueryBuilder;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.jdbc.reader.JdbcReader;
import com.stratio.deep.jdbc.writer.JdbcWriter;
import org.apache.spark.Partition;

import java.util.List;

/**
 * Created by mariomgal on 09/12/14.
 */
public abstract class JdbcNativeExtractor<T, S extends BaseConfig<T>> implements IExtractor<T, S> {

    private static final long serialVersionUID = -298383130965427783L;

    private JdbcReader jdbcReader;

    private JdbcWriter jdbcWriter;

    @Override
    public Partition[] getPartitions(S config) {
        return new Partition[0];
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
