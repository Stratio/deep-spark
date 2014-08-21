package com.stratio.deep.config;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.exception.DeepIllegalAccessException;
import com.stratio.deep.extractor.utils.ExtractorConfig;
import com.stratio.deep.rdd.IDeepPartition;
import com.stratio.deep.extractor.core.IDeepRecordReader;
import org.apache.hadoop.conf.Configuration;
import sun.awt.image.VolatileSurfaceManager;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Created by rcrespo on 19/08/14.
 */
public class DeepJobConfig<T> implements IDeepJobConfig {

    Map<ExtractorConfig, String> values;

    Class<IDeepJobConfig<T, ?>> config;

    Class entityClass = Cells.class;

    Class RDDClass;

    Class inputFormatClass;
    private String host;
    private String columnFamily;

    public Class getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class entityClass) {
        this.entityClass = entityClass;
    }

    public Map<ExtractorConfig, String> getValues() {
        return values;
    }

    public void setValues(Map<ExtractorConfig, String> values) {
        this.values = values;
    }

    public Class<IDeepJobConfig<T, ?>> getConfig() {
        return config;
    }

    public void setConfig(Class<IDeepJobConfig<T, ?>> config) {
        this.config = config;
    }

    public Class getRDDClass() {
        return RDDClass;
    }

    @Override
    public Method getSaveMethod() throws NoSuchMethodException {
        return null;
    }

    @Override
    public Configuration getHadoopConfiguration() {
        return null;
    }

    public Class getInputFormatClass() {
        return inputFormatClass;
    }

    @Override
    public Class<? extends IDeepPartition> getPatitionClass() {
        return null;
    }

    @Override
    public Class<? extends IDeepRecordReader> getRecordReaderClass() {
        return null;
    }

    public void setInputFormatClass(Class inputFormatClass) {
        this.inputFormatClass = inputFormatClass;
    }

    public void setRDDClass(Class RDDClass) {
        this.RDDClass = RDDClass;

    }


    public int getBisectFactor() { //TODO
        return 0;
    }

    public DeepJobConfig<T> host(String defaultCassandraHost) {
        return null;
    }

    public DeepJobConfig<T> rpcPort(Integer cassandraThriftPort) {

        return null;
    }




    public DeepJobConfig<T> cqlPort(int cassandraCqlPort) {
        return null;
    }

    public DeepJobConfig<T> keyspace(String outputKeyspaceName) {
        return null;
    }

    public DeepJobConfig<T> columnFamily(String outputCql3CollectionColumnFamily) {
        return null;
    }

    public DeepJobConfig<T> batchSize(int i) {
        return null;
    }

    public DeepJobConfig<T> createTableOnWrite(Boolean aTrue) {
        return null;
    }

    public DeepJobConfig<T> initialize() {
        return null;
    }

    @Override
    public DeepJobConfig<T> inputColumns(String... columns) {
        return null;
    }

    @Override
    public  DeepJobConfig<T>  password(String password) {
        return null;
    }

    @Override
    public  DeepJobConfig<T>  username(String username) {
        return null;
    }

    @Override
    public int getPageSize() {
        return 0;
    }

    @Override
    public  DeepJobConfig<T>  customConfiguration(Map customConfiguration) {
        return null;
    }

    public DeepJobConfig<T>  bisectFactor(int testBisectFactor) {
        return null;
    }

    @Override
    public String getPassword() {
        return null;
    }

    @Override
    public Map<String, Cell> columnDefinitions() {
        return null;
    }

    public DeepJobConfig<T>  pageSize(int defaultPageSize) {
        return null;
    }

    public DeepJobConfig<T>  table(String table) {
        return null;
    }

    public String getHost() {
        return host;
    }

    @Override
    public String[] getInputColumns() {
        return new String[0];
    }

    @Override
    public String getUsername() {
        return null;
    }

    public DeepJobConfig<T>  filterByField(String notExistentField, Integer val) {
        return null;
    }
    public DeepJobConfig<T>  filterByField(String notExistentField, String val) {
        return null;
    }

    public String getKeyspace() { return null;

    }



    public Integer getRpcPort() {
        return null;
    }

    public DeepJobConfig<T>  readConsistencyLevel(String s) {
        return null;
    }

    public DeepJobConfig<T>  writeConsistencyLevel(String s) {
        return null;

    }


    public IDeepJobConfig partitioner(String s) {
        return null;
    }

    public String getColumnFamily() {
        return columnFamily;
    }
}
