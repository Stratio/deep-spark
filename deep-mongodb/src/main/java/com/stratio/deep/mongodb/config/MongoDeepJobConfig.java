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

package com.stratio.deep.mongodb.config;

import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.FILTER_QUERY;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.IGNORE_ID_FIELD;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.INPUT_KEY;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.READ_PREFERENCE;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.REPLICA_SET;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.SORT;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.SPLIT_SIZE;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.USE_CHUNKS;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.USE_SHARD;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.USE_SPLITS;
import static com.stratio.deep.commons.extractor.utils.ExtractorConstants.WRITE_MODE;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.config.HadoopConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.filter.FilterType;
import com.stratio.deep.mongodb.extractor.MongoNativeCellExtractor;
import com.stratio.deep.mongodb.extractor.MongoNativeEntityExtractor;

/**
 * The type Mongo deep job config.
 *
 * @param <T> the type parameter
 */
public class MongoDeepJobConfig<T> extends HadoopConfig<T, MongoDeepJobConfig<T>> implements IMongoDeepJobConfig<T,
        MongoDeepJobConfig<T>>,
        Serializable {
    /**
     * The constant serialVersionUID.
     */
    private static final long serialVersionUID = -7179376653643603038L;

    /**
     * configuration to be broadcasted to every spark node
     */
    private transient Configuration configHadoop;

    /**
     * Indicates the replica set's name
     */
    private String replicaSet;

    /**
     * Read Preference primaryPreferred is the recommended read preference. If the primary node go down, can still read
     * from secundaries
     */
    private String readPreference = ReadPreference.nearest().getName();

    /**
     * OPTIONAL filter query
     */
    private DBObject query;

    /**
     * OPTIONAL fields to be returned
     */
    private DBObject fields;

    /**
     * OPTIONAL sorting
     */
    private String sort;

    /**
     * Shard key
     */
    private String inputKey;

    /**
     * The Create input split.
     */
    private boolean createInputSplit = true;

    /**
     * The Use shards.
     */
    private boolean useShards = false;

    /**
     * The Splits use chunks.
     */
    private boolean splitsUseChunks = true;

    /**
     * The Split size.
     */
    private Integer splitSize = 8;

    /**
     * The Custom configuration.
     */
    private Map<String, Serializable> customConfiguration;


    private WriteConcern writeConcern = WriteConcern.NORMAL;


    public MongoDeepJobConfig() {
    }

    /**
     * Instantiates a new Mongo deep job config.
     *
     * @param entityClass the entity class
     */
    public MongoDeepJobConfig(Class<T> entityClass) {
        super(entityClass);
        if (Cells.class.isAssignableFrom(entityClass)) {
            extractorImplClass = MongoNativeCellExtractor.class;
        } else {
            extractorImplClass = MongoNativeEntityExtractor.class;
        }
    }

    /**
     * {@inheritDoc}
     */

    public MongoDeepJobConfig<T> pageSize(int pageSize) {
        this.splitSize = pageSize;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUsername() {
        return username;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> filterQuery(DBObject query) {
        this.query = query;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> filterQuery(QueryBuilder query) {
        this.query = query.get();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> replicaSet(String replicaSet) {
        this.replicaSet = replicaSet;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> database(String database) {
        this.catalog = database;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> collection(String collection) {
        this.table = collection;
        return this;
    }

    @Override
    public String getCollection() {
        return table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> username(String username) {
        this.username = username;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> fields(DBObject fields) {
        this.fields = fields;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> sort(String sort) {
        this.sort = sort;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> sort(DBObject sort) {
        this.sort = sort.toString();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> createInputSplit(boolean createInputSplit) {
        this.createInputSplit = createInputSplit;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> useShards(boolean useShards) {
        this.useShards = useShards;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> splitsUseChunks(boolean splitsUseChunks) {
        this.splitsUseChunks = splitsUseChunks;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> inputKey(String inputKey) {
        this.inputKey = inputKey;
        return this;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> password(String password) {
        this.password = password;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> readPreference(String readPreference) {
        this.readPreference = readPreference;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> ignoreIdField() {
        DBObject bsonFields = fields != null ? fields : new BasicDBObject();
        bsonFields.put("_id", 0);
        fields = bsonFields;
        return this;
    }

    @Override
    public String getDatabase() {
        return catalog;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> initialize() {
        validate();
        super.initialize();
        configHadoop = new JobConf();
        configHadoop = new Configuration();
        StringBuilder connection = new StringBuilder();

        connection.append("mongodb").append(":").append("//");

        if (username != null && password != null) {
            connection.append(username).append(":").append(password).append("@");
        }

        boolean firstHost = true;
        for (String hostName : host) {
            if (!firstHost) {
                connection.append(",");
            }
            connection.append(hostName);
            firstHost = false;
        }

        connection.append("/").append(catalog).append(".").append(table);

        StringBuilder options = new StringBuilder();
        boolean asignado = false;

        if (readPreference != null) {
            asignado = true;
            options.append("?readPreference=").append(readPreference);
        }

        if (replicaSet != null) {
            if (asignado) {
                options.append("&");
            } else {
                options.append("?");
            }
            options.append("replicaSet=").append(replicaSet);
        }

        connection.append(options);

        configHadoop.set(MongoConfigUtil.INPUT_URI, connection.toString());

        configHadoop.set(MongoConfigUtil.OUTPUT_URI, connection.toString());

        configHadoop.set(MongoConfigUtil.INPUT_SPLIT_SIZE, String.valueOf(splitSize));

        if (inputKey != null) {
            configHadoop.set(MongoConfigUtil.INPUT_KEY, inputKey);
        }

        configHadoop.set(MongoConfigUtil.SPLITS_USE_SHARDS, String.valueOf(useShards));

        configHadoop.set(MongoConfigUtil.CREATE_INPUT_SPLITS, String.valueOf(createInputSplit));

        configHadoop.set(MongoConfigUtil.SPLITS_USE_CHUNKS, String.valueOf(splitsUseChunks));

        if (query != null) {
            configHadoop.set(MongoConfigUtil.INPUT_QUERY, query.toString());
        }

        if (fields != null) {
            configHadoop.set(MongoConfigUtil.INPUT_FIELDS, fields.toString());
        }

        if (sort != null) {
            configHadoop.set(MongoConfigUtil.INPUT_SORT, sort);
        }

        if (username != null && password != null) {
            configHadoop.set(MongoConfigUtil.AUTH_URI, connection.toString());
        }

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

    private void concantHostPort(){
        for (int i = 0; i < host.size(); i++) {
            if (host.get(i).indexOf(":") == -1) {
                host.set(i, host.get(i).concat(":").concat(String.valueOf(port)));
            }
        }
    }

    /**
     * validates connection parameters
     */
    private void validate() {

        if (host.isEmpty()) {
            throw new IllegalArgumentException("host cannot be null");
        }
        if (catalog == null) {
            throw new IllegalArgumentException("database cannot be null");
        }
        if (table == null) {
            throw new IllegalArgumentException("collection cannot be null");
        }

        concantHostPort();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongoDeepJobConfig<T> inputColumns(String... columns) {
        DBObject bsonFields = fields != null ? fields : new BasicDBObject();
        boolean isIdPresent = false;
        for (String column : columns) {
            if (column.trim().equalsIgnoreCase("_id")) {
                isIdPresent = true;
            }

            bsonFields.put(column.trim(), 1);
        }
        if (!isIdPresent) {
            bsonFields.put("_id", 0);
        }
        fields = bsonFields;
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
    public MongoDeepJobConfig<T> initialize(ExtractorConfig extractorConfig) {
        super.initialize(extractorConfig);

        Map<String, Serializable> values = extractorConfig.getValues();

        if (values.get(REPLICA_SET) != null) {
            replicaSet(extractorConfig.getString(REPLICA_SET));
        }

        if (values.get(READ_PREFERENCE) != null) {
            readPreference(extractorConfig.getString(READ_PREFERENCE));
        }

        if (values.get(SORT) != null) {
            sort(extractorConfig.getString(SORT));
        }

        if (values.get(FILTER_QUERY) != null) {
            filterQuery(extractorConfig.getFilterArray(FILTER_QUERY));
        }

        if (values.get(INPUT_KEY) != null) {
            inputKey(extractorConfig.getString(INPUT_KEY));
        }

        if (values.get(IGNORE_ID_FIELD) != null && extractorConfig.getBoolean(IGNORE_ID_FIELD) == true) {
            ignoreIdField();
        }

        if (values.get(INPUT_KEY) != null) {
            inputKey(extractorConfig.getString(INPUT_KEY));
        }

        if (values.get(USE_SHARD) != null) {
            useShards(extractorConfig.getBoolean(USE_SHARD));
        }

        if (values.get(USE_SPLITS) != null) {
            createInputSplit(extractorConfig.getBoolean(USE_SPLITS));
        }

        if (values.get(USE_CHUNKS) != null) {
            splitsUseChunks(extractorConfig.getBoolean(USE_CHUNKS));
        }
        if (values.get(SPLIT_SIZE) != null) {
            pageSize(extractorConfig.getInteger(SPLIT_SIZE));
        }

        if (values.get(WRITE_MODE) != null) {
            writeConcern((WriteConcern) extractorConfig.getValue(WriteConcern.class, WRITE_MODE));
        }


        this.initialize();

        return this;
    }

    /**
     * Filter query.
     *
     * @param filters the filters
     * @return the mongo deep job config
     */
    public MongoDeepJobConfig<T> filterQuery(Filter[] filters) {

        if (filters.length > 0) {
            List<BasicDBObject> list = new ArrayList<>();

            QueryBuilder queryBuilder = QueryBuilder.start();
            for (int i = 0; i < filters.length; i++) {
                BasicDBObject bsonObject = new BasicDBObject();

                Filter filter = filters[i];
                if (filter.getFilterType().equals(FilterType.EQ)) {
                    bsonObject.put(filter.getField(), filter.getValue());
                } else {
                    bsonObject.put(filter.getField(),
                            new BasicDBObject("$".concat(filter.getFilterType().getFilterTypeId().toLowerCase()),
                                    filter.getValue()));
                }

                list.add(bsonObject);
            }
            queryBuilder.and(list.toArray(new BasicDBObject[list.size()]));

            filterQuery(queryBuilder);
        }
        return this;

    }

    /**
     * Gets input key.
     *
     * @return the input key
     */
    public String getInputKey() {
        return inputKey;
    }

    /**
     * Sets input key.
     *
     * @param inputKey the input key
     */
    public void setInputKey(String inputKey) {
        this.inputKey = inputKey;
    }

    /**
     * Is create input split.
     *
     * @return the boolean
     */
    public boolean isCreateInputSplit() {
        return createInputSplit;
    }

    /**
     * Sets create input split.
     *
     * @param createInputSplit the create input split
     */
    public void setCreateInputSplit(boolean createInputSplit) {
        this.createInputSplit = createInputSplit;
    }

    /**
     * Is use shards.
     *
     * @return the boolean
     */
    public boolean isUseShards() {
        return useShards;
    }

    /**
     * Sets use shards.
     *
     * @param useShards the use shards
     */
    public void setUseShards(boolean useShards) {
        this.useShards = useShards;
    }

    /**
     * Is splits use chunks.
     *
     * @return the boolean
     */
    public boolean isSplitsUseChunks() {
        return splitsUseChunks;
    }

    /**
     * Sets splits use chunks.
     *
     * @param splitsUseChunks the splits use chunks
     */
    public void setSplitsUseChunks(boolean splitsUseChunks) {
        this.splitsUseChunks = splitsUseChunks;
    }

    /**
     * Gets split size.
     *
     * @return the split size
     */
    public Integer getSplitSize() {
        return splitSize;
    }

    /**
     * Sets split size.
     *
     * @param splitSize the split size
     */
    public void setSplitSize(Integer splitSize) {
        this.splitSize = splitSize;
    }

    /**
     * Gets query.
     *
     * @return the query
     */
    public DBObject getQuery() {
        return query;
    }

    public DBObject getDBFields() {
        return fields;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public MongoDeepJobConfig<T> writeConcern(WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    public MongoDeepJobConfig<T> query(DBObject query) {
        this.query = query;
        return this;
    }

    public String getReplicaSet() {
        return replicaSet;
    }

    public String getReadPreference() {
        return readPreference;
    }

    public String getSort() {
        return sort;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("MongoDeepJobConfig{");
        sb.append("configHadoop=").append(configHadoop);
        sb.append(", replicaSet='").append(replicaSet).append('\'');
        sb.append(", readPreference='").append(readPreference).append('\'');
        sb.append(", query=").append(query);
        sb.append(", fields=").append(fields);
        sb.append(", sort='").append(sort).append('\'');
        sb.append(", inputKey='").append(inputKey).append('\'');
        sb.append(", createInputSplit=").append(createInputSplit);
        sb.append(", useShards=").append(useShards);
        sb.append(", splitsUseChunks=").append(splitsUseChunks);
        sb.append(", splitSize=").append(splitSize);
        sb.append(", writeConcern=").append(writeConcern);
        sb.append(", customConfiguration=").append(customConfiguration);
        sb.append('}');
        sb.append(super.toString());
        return sb.toString();
    }
}
