/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.es.config;

import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.FILTER_QUERY;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.INPUT_COLUMNS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.config.HadoopConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.utils.Utils;
import com.stratio.deep.es.extractor.ESCellExtractor;
import com.stratio.deep.es.extractor.ESEntityExtractor;
import com.stratio.deep.es.utils.UtilES;

/**
 * @param <T>
 */
public class ESDeepJobConfig<T> extends HadoopConfig<T> implements IESDeepJobConfig<T> {
    private static final long serialVersionUID = -7179376653643603038L;


    /**
     * A list of elasticsearch host to connect
     */
    private List<String> hostList = new ArrayList<>();



    private String index;

    private String type;



    private List<String> inputColumns = new ArrayList<>();

    /**
     * OPTIONAL filter query
     */
    private String query;

    private Map<String, Object> customConfiguration;



    /**
     * Default constructor
     */
    public ESDeepJobConfig(Class<T> entityClass) {
        super(entityClass);
        if(Cells.class.isAssignableFrom(entityClass)){
            extractorImplClass = ESCellExtractor.class;
        }else{
            extractorImplClass = ESEntityExtractor.class;
        }
    }


    @Override
    public String getPassword() {
        return password;
    }


    @Override
    public ESDeepJobConfig<T> pageSize(int pageSize) {
        return this;
    }

    @Override
    public Class<T> getEntityClass() {
        return entityClass;
    }

    @Override
    public String getHost() {
        if (hostList.isEmpty()) {
            return null;
        }
        return hostList.get(0);
    }

    @Override
    public String[] getInputColumns() {
        return fields;
    }


    @Override
    public ESDeepJobConfig<T> host(String host) {
        hostList.add(host);
        return this;
    }


    public String getResource(){
        return type!=null?index.concat("/").concat(type):index;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public ESDeepJobConfig<T> initialize() {
        validate();

        configHadoop = new JobConf();
        ((JobConf)configHadoop).setInputFormat(EsInputFormat.class);
        ((JobConf)configHadoop).setOutputFormat(EsOutputFormat.class);

        configHadoop.set(ConfigurationOptions.ES_RESOURCE, getResource());
        configHadoop.set(ConfigurationOptions.ES_FIELD_READ_EMPTY_AS_NULL, "false");

        if (query != null) {
            configHadoop.set(ConfigurationOptions.ES_QUERY, query);
        }

        if(!inputColumns.isEmpty()){
            configHadoop.set(ConfigurationOptions.ES_SCROLL_FIELDS, Utils.splitListByComma(inputColumns));
        }

        configHadoop.set(ConfigurationOptions.ES_NODES, Utils.splitListByComma(hostList));
        configHadoop.set(ConfigurationOptions.ES_PORT, String.valueOf(port));
        configHadoop.set(ConfigurationOptions.ES_INPUT_JSON, "yes");

        // index (default)
        // new data is added while existing data (based on its id) is replaced (reindexed).
        // create
        // adds new data - if the data already exists (based on its id), an exception is thrown.
        //
        // update
        // updates existing data (based on its id). If no data is found, an exception is thrown.
        //
        // upsert
        // known as merge or insert if the data does not exist, updates if the data exists (based on its id).

        // configHadoop.set("es.write.operation","");

        if (customConfiguration != null) {
            Set<Map.Entry<String, Object>> set = customConfiguration.entrySet();
            Iterator<Map.Entry<String, Object>> iterator = set.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                configHadoop.set(entry.getKey(), entry.getValue().toString());
            }

        }
        return this;
    }

    /**
     * validates connection parameters
     */
    private void validate() {
        if (hostList.isEmpty()) {
            throw new IllegalArgumentException("host cannot be null");
        }
        if (index == null) {
            throw new IllegalArgumentException("index cannot be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("type cannot be null");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ESDeepJobConfig<T> inputColumns(String... columns) {
        List<String> fields = new ArrayList<>();
        for (String field : columns){
            fields.add(field.trim());
        }

        inputColumns.addAll(fields);

        return this;
    }

    @Override
    public ESDeepJobConfig<T> password(String password) {
        this.password = password;
        return this;
    }

    @Override
    public ESDeepJobConfig<T> username(String username) {
        this.username = username;
        return this;
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public ESDeepJobConfig<T> database(String database) {
        this.index = database;
        return this;
    }

    @Override
    public ESDeepJobConfig<T> host(List<String> host) {
        hostList.addAll(host);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration getHadoopConfiguration() {
        if (configHadoop == null) {
            initialize();
        }
        return configHadoop;
    }

    @Override
    public ESDeepJobConfig<T> filterQuery(String query) {
        this.query = query;
        return this;
    }

    @Override
    public ESDeepJobConfig<T> sort(String sort) {
        return null;
    }

    @Override
    public ESDeepJobConfig<T> inputKey(String inputKey) {
        return null;
    }

    @Override
    public List<String> getHostList() {
        return hostList;
    }

    @Override
    public ESDeepJobConfig<T> type(String type) {
        this.type=type;
        return this;
    }

    /**
     * Same as type
     * @param type
     * @return
     */
    public ESDeepJobConfig<T> tipe(String type) {
        this.type=type;
        return this;
    }

    @Override
    public ESDeepJobConfig<T> index(String index) {
        this.index=index;
        return this;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public String getNameSpace() {
        if(nameSpace == null){
            nameSpace = new StringBuilder().append(getIndex())
                    .append(".")
                    .append(getType()).toString();
        }
        return nameSpace;
    }

    @Override
    public ESDeepJobConfig<T> customConfiguration(Map<String, Serializable> customConfiguration) {
        return null;
    }

    @Override
    public Map<String, Serializable> getCustomConfiguration() {
        return null;
    }

    @Override
    public ESDeepJobConfig<T> initialize(ExtractorConfig extractorConfig) {


        Map<String, String> values = extractorConfig.getValues();

        if (values.get(ExtractorConstants.USERNAME) != null) {
            username(extractorConfig.getString(ExtractorConstants.USERNAME));
        }

        if (values.get(ExtractorConstants.PASSWORD) != null) {
            password(extractorConfig.getString(ExtractorConstants.PASSWORD));
        }

        if (values.get(ExtractorConstants.HOST) != null) {
            host(extractorConfig.getString(ExtractorConstants.HOST));
        }

        if (values.get(ExtractorConstants.PORT) != null) {
            port(extractorConfig.getInteger(ExtractorConstants.PORT));
        }

        if(values.get(ExtractorConstants.INDEX)!=null){
            index = extractorConfig.getString(ExtractorConstants.INDEX);

        }

        if(values.get(ExtractorConstants.TYPE)!= null){
            type = extractorConfig.getString(ExtractorConstants.TYPE);
        }

        if (values.get(INPUT_COLUMNS) != null) {
            inputColumns(extractorConfig.getStringArray(INPUT_COLUMNS));
        }

        if (values.get(FILTER_QUERY) != null) {
            filterQuery(extractorConfig.getFilterArray(FILTER_QUERY));
        }

        this.initialize();

        return this;
    }

    /**
     * @param filterArray
     */
    private ESDeepJobConfig<T> filterQuery(Filter[] filterArray) {
        query = "{ \"query\" :".concat((UtilES.generateQuery(filterArray)).toString()).concat("}");
        return this;
    }


    public ESDeepJobConfig<T> port(int port) {
        this.port = port;
        return this;
    }

}
