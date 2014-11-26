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
import java.util.Arrays;
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
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.utils.Utils;
import com.stratio.deep.es.extractor.ESCellExtractor;
import com.stratio.deep.es.extractor.ESEntityExtractor;
import com.stratio.deep.es.utils.UtilES;

/**
 * @param <T>
 */
public class ESDeepJobConfig<T> extends HadoopConfig<T, ESDeepJobConfig<T>> implements IESDeepJobConfig<T> {
    private static final long serialVersionUID = -7179376653643603038L;



    /**
     * OPTIONAL filter query
     */
    private String query;

    private Map<String, Serializable> customConfiguration;



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




    public String getResource(){
        return table!=null?catalog.concat("/").concat(table):catalog;
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

        if(inputColumns!=null && inputColumns.length>0){
            configHadoop.set(ConfigurationOptions.ES_SCROLL_FIELDS, Utils.splitListByComma(Arrays.asList
                    (inputColumns)));
        }

        configHadoop.set(ConfigurationOptions.ES_NODES, Utils.splitListByComma(host));
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
            Set<Map.Entry<String, Serializable>> set = customConfiguration.entrySet();
            Iterator<Map.Entry<String, Serializable>> iterator = set.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Serializable> entry = iterator.next();
                configHadoop.set(entry.getKey(), entry.getValue().toString());
            }

        }
        return this;
    }

    /**
     * validates connection parameters
     */
    private void validate() {
        if (host.isEmpty()) {
            throw new IllegalArgumentException("host cannot be null");
        }
        if (catalog == null) {
            throw new IllegalArgumentException("index cannot be null");
        }
        if (table == null) {
            throw new IllegalArgumentException("type cannot be null");
        }
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
    public ESDeepJobConfig<T> type(String type) {
        this.table=type;
        return this;
    }

    /**
     * Same as type
     * @param type
     * @return
     */
    public ESDeepJobConfig<T> tipe(String type) {
        this.table=type;
        return this;
    }

    @Override
    public ESDeepJobConfig<T> index(String index) {
        this.catalog=index;
        return this;
    }

    @Override
    public String getType() {
        return table;
    }

    @Override
    public String getIndex() {
        return catalog;
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
        this.customConfiguration = customConfiguration;
        return this;
    }

    @Override
    public Map<String, Serializable> getCustomConfiguration() {
        return customConfiguration;
    }

    @Override
    public ESDeepJobConfig<T> initialize(ExtractorConfig extractorConfig) {
        super.initialize(extractorConfig);

        Map<String, String> values = extractorConfig.getValues();


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


}
