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

package com.stratio.deep.cql;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.config.impl.GenericDeepJobConfig;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.exception.DeepGenericException;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.exception.DeepIllegalAccessException;
import com.stratio.deep.partition.impl.DeepPartitionLocationComparator;
import com.stratio.deep.utils.Utils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

import static com.stratio.deep.cql.CassandraClientProvider.trySessionForLocation;

/**
 * Implements a cassandra record reader with pagination capabilities.
 * Does not rely on Cassandra's Hadoop CqlPagingRecordReader.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public class DeepRecordReader {
    private static final Logger LOG = LoggerFactory.getLogger(DeepRecordReader.class);

    private static final int DEFAULT_CQL_PAGE_LIMIT = 1000;

    private DeepTokenRange split;
    private RowIterator rowIterator;

    private String cfName;

    // partition keys -- key aliases
    private List<BoundColumn> partitionBoundColumns = new ArrayList<>();

    // cluster keys -- column aliases
    private List<BoundColumn> clusterColumns = new ArrayList<>();

    // cql query select columns
    private String columns;

    // the number of cql rows per page
    private int pageRowSize;

    private IPartitioner partitioner;

    private AbstractType<?> keyValidator;

    private final DeepPartitionLocationComparator comparator = new DeepPartitionLocationComparator();

    private final IDeepJobConfig config;

    private Session session;

    /**
     * public constructor. Takes a list of filters to pass to the underlying datastores.
     *
     * @param config
     */
    public DeepRecordReader(IDeepJobConfig config, DeepTokenRange split) {
        this.config = config;
        initialize(split);
    }

    /**
     * Initialized this object.
     * <p>Creates a new client and row iterator.</p>
     *
     * @param split
     */
    private void initialize(DeepTokenRange split){
        this.split = split;

        cfName = config.getTable();

        if (!ArrayUtils.isEmpty(config.getInputColumns())) {
            columns = StringUtils.join(config.getInputColumns(), ",");
        }

        pageRowSize = DEFAULT_CQL_PAGE_LIMIT;

        partitioner = Utils.newTypeInstance(config.getPartitionerClassName(), IPartitioner.class);

        try {
            session = createConnection();

            retrieveKeys();
        } catch (Exception e) {
            throw new DeepIOException(e);
        }

        rowIterator = new RowIterator();

        LOG.debug("created {}", rowIterator);
    }

    private Session createConnection() throws Exception {

        /* reorder locations */
        Iterable<String> locations =
                Ordering.from(new DeepPartitionLocationComparator()).sortedCopy(split.getReplicas());
        
        Exception lastException = null;


        LOG.info("createConnection: " + locations);
        for (String location : locations) {

            try {
                return trySessionForLocation(location, config, false).left;
            } catch (Exception e) {
                LOG.error("Could not get connection for: {}, replicas: {}", location, locations);
                lastException = e;
            }
        }

        throw lastException;
    }

    /**
     * Closes this input reader object.
     */
    public void close() {
    }

    /**
     * Creates a new key map.
     *
     * @return
     */
    public Map<String, ByteBuffer> createKey() {
        return new LinkedHashMap<String, ByteBuffer>();
    }

    /**
     * Creates a new value map.
     *
     * @return
     */
    public Map<String, ByteBuffer> createValue() {
        return new LinkedHashMap<String, ByteBuffer>();
    }

    /**
     * CQL row iterator
     */
    class RowIterator extends AbstractIterator<Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>>> {
        private int totalRead = 0;             // total number of cf rows read
        private Iterator<Row> rows;
        private int pageRows = 0;                // the number of cql rows read of this page
        private String previousRowKey = null;    // previous CF row key
        private String partitionKeyString;       // keys in <key1>, <key2>, <key3> string format
        private String partitionKeyMarkers;      // question marks in ? , ? , ? format which matches the number of keys

        /**
         * Default constructor.
         */
        public RowIterator() {
            // initial page
            executeQuery();
        }

        /**
         * {@inheritDoc}
         */
        protected Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> computeNext() {
            if (rows == null) {
                return endOfData();
            }

            int index = -2;
            //check there are more page to read
            while (!rows.hasNext()) {
                // no more data
                if (index == -1 || emptyPartitionKeyValues()) {
                    LOG.debug("no more data");
                    return endOfData();
                }

                index = setTailNull(clusterColumns);
                LOG.debug("set tail to null, index: {}", index);
                executeQuery();
                pageRows = 0;

                if (rows == null || !rows.hasNext() && index < 0) {
                    LOG.debug("no more data");
                    return endOfData();
                }
            }

            Map<String, ByteBuffer> valueColumns = createValue();
            Map<String, ByteBuffer> keyColumns = createKey();
            Row row = rows.next();
            TableMetadata tableMetadata = ((GenericDeepJobConfig) config).fetchTableMetadata();

            List<ColumnMetadata> partitionKeys = tableMetadata.getPartitionKey();
            List<ColumnMetadata> clusteringKeys = tableMetadata.getClusteringColumns();
            List<ColumnMetadata> allColumns = tableMetadata.getColumns();

            for (ColumnMetadata key : partitionKeys) {
                String columnName = key.getName();
                ByteBuffer bb = row.getBytesUnsafe(columnName);
                keyColumns.put(columnName, bb);
            }
            for (ColumnMetadata key : clusteringKeys) {
                String columnName = key.getName();
                ByteBuffer bb = row.getBytesUnsafe(columnName);
                keyColumns.put(columnName, bb);
            }
            for (ColumnMetadata key : allColumns) {
                String columnName = key.getName();
                if (keyColumns.containsKey(columnName)){
                    continue;
                }

                ByteBuffer bb = row.getBytesUnsafe(columnName);
                valueColumns.put(columnName, bb);
            }

            // increase total CQL row read for this page
            pageRows++;

            // increase total CF row read
            if (newRow(keyColumns, previousRowKey)) {
                totalRead++;
            }

            // read full page
            if (pageRows >= pageRowSize || !rows.hasNext()) {
                Iterator<String> newKeys = keyColumns.keySet().iterator();
                for (BoundColumn column : partitionBoundColumns) {
                    column.value = keyColumns.get(newKeys.next());
                }

                for (BoundColumn column : clusterColumns) {
                    column.value = keyColumns.get(newKeys.next());
                }

                executeQuery();
                pageRows = 0;
            }

            return Pair.create(keyColumns, valueColumns);
        }

        /**
         * check whether start to read a new CF row by comparing the partition keys
         */
        private boolean newRow(Map<String, ByteBuffer> keyColumns, String previousRowKey) {
            if (keyColumns.isEmpty()) {
                return false;
            }

            String rowKey = "";
            if (keyColumns.size() == 1) {
                rowKey = partitionBoundColumns.get(0).validator.getString(keyColumns.get(partitionBoundColumns.get(0).name));
            } else {
                Iterator<ByteBuffer> iter = keyColumns.values().iterator();

                for (BoundColumn column : partitionBoundColumns) {
                    rowKey = rowKey + column.validator.getString(ByteBufferUtil.clone(iter.next())) + ":";
                }
            }

            LOG.debug("previous RowKey: {}, new row key: {}", previousRowKey, rowKey);
            if (previousRowKey == null) {
                this.previousRowKey = rowKey;
                return true;
            }

            if (rowKey.equals(previousRowKey)) {
                return false;
            }

            this.previousRowKey = rowKey;
            return true;
        }

        /**
         * set the last non-null key value to null, and return the previous index
         */
        private int setTailNull(List<BoundColumn> values) {
            if (values.isEmpty()) {
                return -1;
            }

            Iterator<BoundColumn> iterator = values.iterator();
            int previousIndex = -1;
            BoundColumn current;
            while (iterator.hasNext()) {
                current = iterator.next();
                if (current.value == null) {
                    int index = previousIndex > 0 ? previousIndex : 0;
                    BoundColumn column = values.get(index);
                    LOG.debug("set key {} value to  null", column.name);
                    column.value = null;
                    return previousIndex - 1;
                }

                previousIndex++;
            }

            BoundColumn column = values.get(previousIndex);
            LOG.debug("set key {} value to  null", column.name);
            column.value = null;
            return previousIndex - 1;
        }

        /**
         * serialize the prepared query, pair.left is query id, pair.right is query
         */
        private Pair<Integer, String> composeQuery(String columns) {
            Pair<Integer, String> clause = whereClause();
            if (columns == null) {
                columns = "*";
            } else {
                // add keys in the front in order
                String partitionKey = keyString(partitionBoundColumns);
                String clusterKey = keyString(clusterColumns);

                columns = withoutKeyColumns(columns);
                columns = (clusterKey == null || "".equals(clusterKey))
                    ? partitionKey + (columns != null ? ("," + columns) : "")
                    : partitionKey + "," + clusterKey + (columns != null ? ("," + columns) : "");
            }

            return Pair.create(clause.left,
                String.format("SELECT %s FROM %s%s%s LIMIT %d ALLOW FILTERING",
                    columns,
                    quote(cfName),
                    clause.right,
                    Utils.additionalFilterGenerator(config.getAdditionalFilters()),
                    pageRowSize));
        }

        /**
         * remove key columns from the column string
         */
        private String withoutKeyColumns(String columnString) {
            Set<String> keyNames = new HashSet<>();
            for (BoundColumn column : Iterables.concat(partitionBoundColumns, clusterColumns)) {
                keyNames.add(column.name);
            }

            String[] columns = columnString.split(",");
            String result = null;
            for (String column : columns) {
                String trimmed = column.trim();
                if (keyNames.contains(trimmed)) {
                    continue;
                }

                String quoted = quote(trimmed);
                result = result == null ? quoted : result + "," + quoted;
            }
            return result;
        }

        /**
         * serialize the where clause
         */
        private Pair<Integer, String> whereClause() {
            if (partitionKeyString == null) {
                partitionKeyString = keyString(partitionBoundColumns);
            }

            if (partitionKeyMarkers == null) {
                partitionKeyMarkers = partitionKeyMarkers();
            }
            // initial query token(k) >= start_token and token(k) <= end_token
            if (emptyPartitionKeyValues()) {
                return Pair.create(0, String.format(" WHERE token(%s) > ? AND token(%s) <= ?", partitionKeyString, partitionKeyString));
            }

            // query token(k) > token(pre_partition_key) and token(k) <= end_token
            if (clusterColumns.size() == 0 || clusterColumns.get(0).value == null) {
                return Pair.create(1,
                    String.format(" WHERE token(%s) > token(%s)  AND token(%s) <= ?",
                        partitionKeyString, partitionKeyMarkers, partitionKeyString));
            }
            // query token(k) = token(pre_partition_key) and m = pre_cluster_key_m and n > pre_cluster_key_n
            Pair<Integer, String> clause = whereClause(clusterColumns, 0);
            return Pair.create(clause.left,
                String.format(" WHERE token(%s) = token(%s) %s", partitionKeyString, partitionKeyMarkers, clause.right));
        }

        /**
         * recursively serialize the where clause
         */
        private Pair<Integer, String> whereClause(List<BoundColumn> column, int position) {
            if (position == column.size() - 1 || column.get(position + 1).value == null) {
                return Pair.create(position + 2, String.format(" AND %s > ? ", quote(column.get(position).name)));
            }

            Pair<Integer, String> clause = whereClause(column, position + 1);
            return Pair.create(clause.left, String.format(" AND %s = ? %s", quote(column.get(position).name), clause.right));
        }

        /**
         * check whether all key values are null
         */
        private boolean emptyPartitionKeyValues() {
            for (BoundColumn column : partitionBoundColumns) {
                if (column.value != null) {
                    return false;
                }
            }
            return true;
        }

        /**
         * serialize the partition key string in format of <key1>, <key2>, <key3>
         */
        private String keyString(List<BoundColumn> columns) {
            String result = null;
            for (BoundColumn column : columns) {
                result = result == null ? quote(column.name) : result + "," + quote(column.name);
            }

            return result == null ? "" : result;
        }

        /**
         * serialize the question marks for partition key string in format of ?, ? , ?
         */
        private String partitionKeyMarkers() {
            String result = null;
            for (BoundColumn column : partitionBoundColumns) {
                result = result == null ? "?" : result + ",?";
            }

            return result;
        }

        /**
         * serialize the query binding variables, pair.left is query id, pair.right is the binding variables
         */
        private Pair<Integer, List<Object>> preparedQueryBindValues() {
            List<Object> values = new LinkedList<>();

            AbstractType<?> tkValidator = partitioner.getTokenValidator();
            Object startToken = split.getStartToken();
            Object endToken = split.getEndToken();

            // initial query token(k) >= start_token and token(k) <= end_token
            if (emptyPartitionKeyValues()) {
                values.add(startToken);
                values.add(endToken);
                return Pair.create(0, values);
            } else {
                for (BoundColumn bColumn : partitionBoundColumns) {
                    values.add(bColumn.validator.compose(bColumn.value));
                }

                if (clusterColumns.size() == 0 || clusterColumns.get(0).value == null) {
                    // query token(k) > token(pre_partition_key) and token(k) <= end_token
                    values.add(endToken);
                    return Pair.create(1, values);
                } else {
                    // query token(k) = token(pre_partition_key) and m = pre_cluster_key_m and n > pre_cluster_key_n
                    int type = preparedQueryBindValues(clusterColumns, 0, values);
                    return Pair.create(type, values);
                }
            }
        }

        /**
         * recursively serialize the query binding variables
         */
        private int preparedQueryBindValues(List<BoundColumn> columns, int position, List<Object> bindValues) {
            AbstractType<?> tkValidator = partitioner.getTokenValidator();

            BoundColumn boundColumn = columns.get(position);
            Object boundColumnValue = boundColumn.validator.compose(boundColumn.value);
            if (position == columns.size() - 1 || columns.get(position+1).value == null) {
                bindValues.add(boundColumnValue);
                return position + 2;
            } else {
                bindValues.add(boundColumnValue);
                return preparedQueryBindValues(columns, position + 1, bindValues);
            }
        }

        /**
         * Quoting for working with uppercase
         */
        private String quote(String identifier) {
            return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
        }

        /**
         * execute the prepared query
         */
        private void executeQuery() {
            Pair<Integer, String> query = composeQuery(columns);

            Pair<Integer, List<Object>> bindValues = null;
            try {
                bindValues = preparedQueryBindValues();
            } catch (Exception e) {
                LOG.error("Exception",e);
            }
            LOG.debug("query type: {}", bindValues.left);

            // check whether it reach end of range for type 1 query CASSANDRA-5573
            if (bindValues.left == 1 && reachEndRange()) {
                rows = null;
                return;
            }

            int retries = 0;

            Exception exception = null;
            // only try three times for TimedOutException and UnavailableException
            while (retries < 3) {
                try {
                    //Session session = createConnection();

                    Object[] values = bindValues.right.toArray(new Object[bindValues.right.size()]);

                    LOG.debug("> Executing query {{}}; bind vars {}", query.right, values);
                    ResultSet resultSet = session.execute(query.right, values);

                    if (resultSet != null) {
                        rows = resultSet.iterator();
                    }
                    return;
                } catch (NoHostAvailableException e){
                    LOG.error("Could not connect to ");
                    exception = e;

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e1) {}

                    ++retries;

                } catch (Exception e) {
                    throw new DeepIOException(e);
                }
            }

            if (exception != null){
                throw new DeepIOException(exception);
            }
        }
    }

    /**
     * retrieve the partition keys and cluster keys from system.schema_columnfamilies table
     */
    private void retrieveKeys() throws Exception {
        TableMetadata tableMetadata = ((GenericDeepJobConfig) config).fetchTableMetadata();

        List<ColumnMetadata> partitionKeys = tableMetadata.getPartitionKey();
        List<ColumnMetadata> clusteringKeys = tableMetadata.getClusteringColumns();

        List<AbstractType<?>> types = new ArrayList<>();

        for (ColumnMetadata key : partitionKeys) {
            String columnName = key.getName();
            BoundColumn boundColumn = new BoundColumn(columnName);
            boundColumn.validator = Cell.getValueType(key.getType()).getAbstractType();
            partitionBoundColumns.add(boundColumn);
            types.add(boundColumn.validator);
        }
        for (ColumnMetadata key : clusteringKeys) {
            String columnName = key.getName();
            BoundColumn boundColumn = new BoundColumn(columnName);
            boundColumn.validator = Cell.getValueType(key.getType()).getAbstractType();
            clusterColumns.add(boundColumn);
        }

        if (types.size()>1){
            keyValidator = CompositeType.getInstance(types);
        } else if (types.size() == 1) {
            keyValidator = types.get(0);
        } else{
            throw new DeepGenericException("Cannot determine if keyvalidator is composed or not, partitionKeys: "+ partitionKeys);
        }
    }

    /**
     * check whether current row is at the end of range
     */
    private boolean reachEndRange() {
        // current row key
        ByteBuffer rowKey;

        if (keyValidator instanceof CompositeType) {
            ByteBuffer[] keys = new ByteBuffer[partitionBoundColumns.size()];
            for (int i = 0; i < partitionBoundColumns.size(); i++) {
                keys[i] = partitionBoundColumns.get(i).value.duplicate();
            }

            rowKey = CompositeType.build(keys);
        } else {
            rowKey = partitionBoundColumns.get(0).value;
        }

        String endToken = String.valueOf(split.getEndToken());
        String currentToken = partitioner.getToken(rowKey).toString();
        LOG.debug("End token: {}, current token: {}", endToken, currentToken);

        return endToken.equals(currentToken);
    }

    private static class BoundColumn {
        private final String name;
        private ByteBuffer value;
        private AbstractType<?> validator;

        public BoundColumn(String name) {
            this.name = name;
        }
    }

    /**
     * Returns a boolean indicating if the underlying rowIterator has a new element or not.
     * DOES NOT advance the iterator to the next element.
     *
     * @return
     */
    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    /**
     * Returns the next element in the underlying rowIterator.
     *
     * @return
     */
    public Pair<Map<String, ByteBuffer>, Map<String, ByteBuffer>> next() {
        if (!this.hasNext()) {
            throw new DeepIllegalAccessException("DeepRecordReader exhausted");
        }
        return rowIterator.next();
    }
}
