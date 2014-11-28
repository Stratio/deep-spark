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

package com.stratio.deep.cassandra.util;

import static com.stratio.deep.cassandra.util.AnnotationUtils.MAP_JAVA_TYPE_TO_ABSTRACT_TYPE;
import static com.stratio.deep.commons.utils.AnnotationUtils.deepFieldName;
import static com.stratio.deep.commons.utils.AnnotationUtils.getBeanFieldValue;
import static com.stratio.deep.commons.utils.Utils.quote;
import static com.stratio.deep.commons.utils.Utils.singleQuote;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Token;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.deep.cassandra.config.CassandraDeepJobConfig;
import com.stratio.deep.cassandra.config.ICassandraDeepJobConfig;
import com.stratio.deep.cassandra.config.OperatorCassandra;
import com.stratio.deep.cassandra.cql.DeepCqlRecordWriter;
import com.stratio.deep.cassandra.querybuilder.DefaultQueryBuilder;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.entity.IDeepType;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.filter.FilterType;
import com.stratio.deep.commons.functions.AbstractSerializableFunction2;
import com.stratio.deep.commons.rdd.DeepTokenRange;
import com.stratio.deep.commons.utils.AnnotationUtils;
import com.stratio.deep.commons.utils.Pair;
import com.stratio.deep.commons.utils.Utils;

import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * Created by luca on 16/04/14.
 */
public class CassandraUtils {

    /**
     * private constructor
     */
    CassandraUtils() {
    }

    public static <W> void doCql3SaveToCassandra(RDD<W> rdd, ICassandraDeepJobConfig<W> writeConfig,
                                                 Function1<W, Tuple2<Cells, Cells>> transformer) {
        if (!writeConfig.getIsWriteConfig()) {
            throw new IllegalArgumentException("Provided configuration object is not suitable for writing");
        }
        Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> tuple = new Tuple2<>(null, null);

        RDD<Tuple2<Cells, Cells>> mappedRDD = rdd.map(transformer,
                ClassTag$.MODULE$.<Tuple2<Cells, Cells>>apply(tuple.getClass()));

        ((CassandraDeepJobConfig) writeConfig).createOutputTableIfNeeded(mappedRDD.first());

        final int pageSize = writeConfig.getBatchSize();
        int offset = 0;

        List<Tuple2<Cells, Cells>> elements = Arrays.asList((Tuple2<Cells, Cells>[]) mappedRDD.collect());
        List<Tuple2<Cells, Cells>> split;
        do {
            split = elements.subList(pageSize * (offset++), Math.min(pageSize * offset, elements.size()));

            Batch batch = QueryBuilder.batch();

            for (Tuple2<Cells, Cells> t : split) {
                Tuple2<String[], Object[]> bindVars = Utils.prepareTuple4CqlDriver(t);

                Insert insert = QueryBuilder
                        .insertInto(quote(writeConfig.getKeyspace()), quote(writeConfig.getTable()))
                        .values(bindVars._1(), bindVars._2());

                batch.add(insert);
            }
            writeConfig.getSession().execute(batch);

        } while (!split.isEmpty() && split.size() == pageSize);
    }

    /**
     * Provided the mapping function <i>transformer</i> that transforms a generic RDD to an RDD<Tuple2<Cells, Cells>>,
     * this generic method persists the RDD to underlying Cassandra datastore.
     *
     * @param rdd
     * @param writeConfig
     * @param transformer
     */
    public static <W> void doSaveToCassandra(RDD<W> rdd, final ICassandraDeepJobConfig<W> writeConfig,
                                             Function1<W, Tuple2<Cells, Cells>> transformer) {

        if (!writeConfig.getIsWriteConfig()) {
            throw new IllegalArgumentException("Provided configuration object is not suitable for writing");
        }

        Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> tuple = new Tuple2<>(null, null);

        final RDD<Tuple2<Cells, Cells>> mappedRDD = rdd.map(transformer,
                ClassTag$.MODULE$.<Tuple2<Cells, Cells>>apply(tuple.getClass()));

        ((CassandraDeepJobConfig) writeConfig).createOutputTableIfNeeded(mappedRDD.first());

        ClassTag<Integer> uClassTag = ClassTag$.MODULE$.apply(Integer.class);

        mappedRDD.context().runJob(mappedRDD,
                new AbstractSerializableFunction2<TaskContext, Iterator<Tuple2<Cells, Cells>>, Integer>() {

                    @Override
                    public Integer apply(TaskContext context, Iterator<Tuple2<Cells, Cells>> rows) {

                        try (DeepCqlRecordWriter writer = new DeepCqlRecordWriter(writeConfig,
                                new DefaultQueryBuilder())) {
                            while (rows.hasNext()) {
                                Tuple2<Cells, Cells> row = rows.next();
                                writer.write(row._1(), row._2());
                            }
                        }

                        return null;
                    }
                }, uClassTag
        );

    }

    /**
     * Returns an instance of the Cassandra validator that matches the provided object.
     *
     * @param obj the object to use to resolve the cassandra marshaller.
     * @param <T> the generic object type.
     * @return an instance of the Cassandra validator that matches the provided object.
     * @throws com.stratio.deep.commons.exception.DeepGenericException if no validator can be found for the specified object.
     */
    public static <T> AbstractType<?> marshallerInstance(T obj) {

        AbstractType<?> abstractType = null;

        if (obj != null) {
            abstractType = MAP_JAVA_TYPE_TO_ABSTRACT_TYPE.get(obj.getClass());

            if (obj instanceof UUID) {
                UUID uuid = (UUID) obj;

                if (uuid.version() == 1) {
                    abstractType = TimeUUIDType.instance;

                } else {
                    abstractType = UUIDType.instance;
                }
            }

            if (abstractType == null) {
                //LIST Case
                if (List.class.isAssignableFrom(obj.getClass())) {

                    List list = (List) obj;
                    if (!list.isEmpty()) {
                        abstractType = ListType.getInstance(marshallerInstance(list.get(0)));
                    }

                }
                // SET Case
                else if (Set.class.isAssignableFrom(obj.getClass())) {
                    Set set = (Set) obj;
                    if (!set.isEmpty()) {
                        java.util.Iterator i = set.iterator();
                        Object o = i.next();
                        abstractType = SetType.getInstance(marshallerInstance(o));
                    }
                }
                // MAP Case
                else if (Map.class.isAssignableFrom(obj.getClass())) {
                    Set set = ((Map) obj).keySet();
                    if (!set.isEmpty()) {
                        java.util.Iterator i = set.iterator();
                        Object o = i.next();
                        abstractType = MapType
                                .getInstance(marshallerInstance(o), marshallerInstance(((Map) obj).get(o)));

                    }

                }
            }

        }

        if (abstractType == null) {
            throw new DeepGenericException(
                    "parameter class " + obj.getClass().getCanonicalName() + " does not have a" +
                            " Cassandra marshaller");
        }

        return abstractType;
    }

    /**
     * Generates the update query for the provided IDeepType. The UPDATE query takes into account all the columns of the
     * testentity, even those containing the null value. We do not generate the key part of the update query. The
     * provided query will be concatenated with the key part by CqlRecordWriter.
     *
     * @param keys               the row keys wrapped inside a Cells object.
     * @param values             all the other row columns wrapped inside a Cells object.
     * @param outputKeyspace     the output keyspace.
     * @param outputColumnFamily the output column family.
     * @return the update query statement.
     */
    public static String updateQueryGenerator(Cells keys, Cells values, String outputKeyspace,
                                              String outputColumnFamily) {

        StringBuilder sb = new StringBuilder("UPDATE ").append(outputKeyspace).append(".").append(outputColumnFamily)
                .append(" SET ");

        int k = 0;

        StringBuilder keyClause = new StringBuilder(" WHERE ");
        for (Cell cell : keys.getCells()) {
            if (((Cell) cell).isKey() || cell.isClusterKey()) {
                if (k > 0) {
                    keyClause.append(" AND ");
                }

                keyClause.append(String.format("%s = ?", quote(cell.getCellName())));

                ++k;
            }

        }

        k = 0;
        for (Cell cell : values.getCells()) {
            if (k > 0) {
                sb.append(", ");
            }

            sb.append(String.format("%s = ?", quote(cell.getCellName())));
            ++k;
        }

        sb.append(keyClause).append(";");

        return sb.toString();
    }

    /**
     * Generates a create table cql statement from the given Cells description.
     *
     * @param keys               the row keys wrapped inside a Cells object.
     * @param values             all the other row columns wrapped inside a Cells object.
     * @param outputKeyspace     the output keyspace.
     * @param outputColumnFamily the output column family.
     * @return the create table statement.
     */
    public static String createTableQueryGenerator(Cells keys, Cells values, String outputKeyspace,
                                                   String outputColumnFamily) {

        if (keys == null || StringUtils.isEmpty(outputKeyspace)
                || StringUtils.isEmpty(outputColumnFamily)) {
            throw new DeepGenericException("keys, outputKeyspace and outputColumnFamily cannot be null");
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(outputKeyspace)
                .append(".").append(outputColumnFamily).append(" (");

        List<String> partitionKey = new ArrayList<>();
        List<String> clusterKey = new ArrayList<>();

        boolean isFirstField = true;

        for (Cell key : keys) {

            String cellName = quote(key.getCellName());

            if (!isFirstField) {
                sb.append(", ");
            }

            // CellValidator cellValidator = CellValidator.cellValidator(key.getCellValue());
            sb.append(cellName).append(" ")
                    .append(CassandraUtils.marshallerInstance(key.getValue()).asCQL3Type().toString());

            if (((Cell) key).isKey()) {
                partitionKey.add(cellName);
            } else if (((Cell) key).isClusterKey()) {
                clusterKey.add(cellName);
            }

            isFirstField = false;
        }

        if (values != null) {
            for (Cell cell : values) {
                sb.append(", ");
                if (cell.getValue() != null) {
                    sb.append(quote(cell.getCellName())).append(" ")
                            .append(CassandraUtils.marshallerInstance(cell.getValue()).asCQL3Type().toString());
                }
            }
        }

        StringBuilder partitionKeyToken = new StringBuilder("(");

        isFirstField = true;
        for (String s : partitionKey) {
            if (!isFirstField) {
                partitionKeyToken.append(", ");
            }
            partitionKeyToken.append(s);
            isFirstField = false;
        }

        partitionKeyToken.append(")");

        StringBuilder clusterKeyToken = new StringBuilder("");

        isFirstField = true;
        for (String s : clusterKey) {
            if (!isFirstField) {
                clusterKeyToken.append(", ");
            }
            clusterKeyToken.append(s);
            isFirstField = false;
        }

        StringBuilder keyPart = new StringBuilder(", PRIMARY KEY ");

        if (!clusterKey.isEmpty()) {
            keyPart.append("(");
        }

        keyPart.append(partitionKeyToken);

        if (!clusterKey.isEmpty()) {
            keyPart.append(", ");
            keyPart.append(clusterKeyToken);
            keyPart.append(")");
        }

        sb.append(keyPart).append(");");

        return sb.toString();
    }

    /**
     * Convers an instance of type <T> to a tuple of ( Map<String, ByteBuffer>, List<ByteBuffer> ). The first map
     * contains the key column names and the corresponding values. The ByteBuffer list contains the value of the columns
     * that will be bounded to CQL query parameters.
     *
     * @param e   the entity object to process.
     * @param <T> the entity object generic type.
     * @return a pair whose first element is a Cells object containing key Cell(s) and whose second element contains all
     * of the other Cell(s).
     */
    public static <T extends IDeepType> Tuple2<Cells, Cells> deepType2tuple(T e) {

        Pair<Field[], Field[]> fields = AnnotationUtils.filterKeyFields(e.getClass());

        Field[] keyFields = fields.left;
        Field[] otherFields = fields.right;

        Cells keys = new Cells(e.getClass().getName());
        Cells values = new Cells(e.getClass().getName());

        for (Field keyField : keyFields) {
            keys.add(createFromEntity(e, keyField));
        }
        for (Field valueField : otherFields) {
            values.add(createFromEntity(e, valueField));
        }

        return new Tuple2<>(keys, values);
    }

    /**
     * Generates the part of the query where clause that will hit the Cassandra's secondary indexes.
     *
     * @param additionalFilters the map of filters names and values.
     * @return the query subpart corresponding to the provided additional filters.
     */
    public static String additionalFilterGenerator(Map<String, Serializable> additionalFilters, Filter[] filters,
                                                   String luceneIndex) {

        StringBuilder sb = new StringBuilder("");

        if (!MapUtils.isEmpty(additionalFilters)) {
            for (Map.Entry<String, Serializable> entry : additionalFilters.entrySet()) {
                if (entry.getValue() == null) {
                    continue;
                }

                String value = entry.getValue().toString();

                if (entry.getValue() instanceof String) {
                    value = singleQuote(value.trim());
                }

                sb.append(" AND ").append(quote(entry.getKey())).append(" = ").append(value);
            }
        }

        if (filters != null) {
            for (int i = 0; i < filters.length; i++) {

                FilterType filterType = filters[i].getFilterType();

                String value = filters[i].getValue().toString();

                if (filters[i].getValue() instanceof String) {
                    value = singleQuote(value.trim());
                }

                switch (filterType) {

                case IN:
                    List<String> inValues = (List<String>) filters[i].getValue();

                    sb.append(" AND ")
                            .append(quote(filters[i].getField()))
                            .append(" IN ").append("(");

                    if (!inValues.isEmpty()) {
                        if (inValues.get(0) instanceof String) {
                            sb.append("'").append(StringUtils.join(((List<String>) filters[i].getValue()), "','"))
                                    .append("'");
                        } else {
                            sb.append(StringUtils.join(((List<String>) filters[i].getValue()), ","));
                        }
                    }
                    sb.append(")");
                    break;
                case BETWEEN:
                    break;
                case MATCH:
                    sb.append(" AND ").append(luceneIndex).append(" = '");
                    sb.append(getLuceneWhereClause(filters[i]));
                    sb.append("'");
                    break;
                case NEQ:
                    sb.append(" AND ").append(quote(filters[i].getField())).append(" ")
                            .append(" < ")
                            .append(" ").append(value)
                            .append(" AND ").append(quote(filters[i].getField())).append(" ")
                            .append(" > ")
                            .append(" ").append(value);
                    break;
                default:
                    sb.append(" AND ").append(quote(filters[i].getField())).append(" ")
                            .append(OperatorCassandra.getOperatorCassandra(filters[i].getFilterType()).getOperator())
                            .append(" ").append(value);
                    break;
                }
            }
        }

        return sb.toString();
    }

    /**
     * Generates the part of the query where clause that will hit the Cassandra's secondary indexes.
     *
     * @param additionalFilters the map of filters names and values.
     * @return the query subpart corresponding to the provided additional filters.
     */
    public static String additionalFilterGenerator(Map<String, Serializable> additionalFilters) {
        if (MapUtils.isEmpty(additionalFilters)) {
            return "";
        }

        StringBuilder sb = new StringBuilder("");

        for (Map.Entry<String, Serializable> entry : additionalFilters.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }

            String value = entry.getValue().toString();

            if (entry.getValue() instanceof String) {
                value = singleQuote(value.trim());
            }

            sb.append(" AND ").append(quote(entry.getKey())).append(" = ").append(value);
        }

        return sb.toString();
    }

    private static String getLuceneWhereClause(Filter filter) {
        String result;

        StringBuilder sb = new StringBuilder("{filter:{type:\"boolean\",must:[");

        String column = filter.getField();
        String value = (String) filter.getValue();

        // Generate query for column
        String[] processedQuery = processLuceneQueryType(value);
        sb.append("{type:\"");
        sb.append(processedQuery[0]);
        sb.append("\",field:\"");
        sb.append(column);
        sb.append("\",value:\"");
        sb.append(processedQuery[1]);
        sb.append("\"},");

        sb.replace(sb.length() - 1, sb.length(), "");
        sb.append("]}}");

        result = sb.toString();

        return result;

    }

    /**
     * Process a query pattern to determine the type of Lucene query. The supported types of queries are: <li>
     * <ul>
     * Wildcard: The query contains * or ?.
     * </ul>
     * <ul>
     * Fuzzy: The query ends with ~ and a number.
     * </ul>
     * <ul>
     * Regex: The query contains [ or ].
     * </ul>
     * <ul>
     * Match: Default query, supporting escaped symbols: *, ?, [, ], etc.
     * </ul>
     * </li>
     *
     * @param query The user query.
     * @return An array with the type of query and the processed query.
     */
    private static String[] processLuceneQueryType(String query) {
        String[] result = { "", "" };
        Pattern escaped = Pattern.compile(".*\\\\\\*.*|.*\\\\\\?.*|.*\\\\\\[.*|.*\\\\\\].*");
        Pattern wildcard = Pattern.compile(".*\\*.*|.*\\?.*");
        Pattern regex = Pattern.compile(".*\\].*|.*\\[.*");
        Pattern fuzzy = Pattern.compile(".*~\\d+");
        if (escaped.matcher(query).matches()) {
            result[0] = "match";
            result[1] =
                    query.replace("\\*", "*").replace("\\?", "?").replace("\\]", "]")
                            .replace("\\[", "[");
        } else if (regex.matcher(query).matches()) {
            result[0] = "regex";
            result[1] = query;
        } else if (fuzzy.matcher(query).matches()) {
            result[0] = "fuzzy";
            result[1] = query;
        } else if (wildcard.matcher(query).matches()) {
            result[0] = "wildcard";
            result[1] = query;
        } else {
            result[0] = "match";
            result[1] = query;
        }
        // C* Query builder doubles the ' character.
        result[1] = result[1].replaceAll("^'", "").replaceAll("'$", "");
        return result;
    }

    /**
     * Returns the partition key related to a given {@link Cells}.
     *
     * @param cells        {@link Cells} from Cassandra to extract the partition key.
     * @param keyValidator Cassandra key type.
     * @param numberOfKeys Number of keys.
     * @return Partition key.
     */
    public static ByteBuffer getPartitionKey(Cells cells, AbstractType<?> keyValidator, int numberOfKeys) {
        ByteBuffer partitionKey;
        if (keyValidator instanceof CompositeType) {
            ByteBuffer[] keys = new ByteBuffer[numberOfKeys];

            for (int i = 0; i < cells.size(); i++) {
                Cell c = cells.getCellByIdx(i);

                if (c.isKey()) {
                    keys[i] = DataType.serializeValue(c.getValue(), ProtocolVersion.V2);
                }
            }

            partitionKey = CompositeType.build(keys);
        } else {
            Cell cell = cells.getCellByIdx(0);
            partitionKey = DataType.serializeValue(cell.getValue(), ProtocolVersion.V2);
        }
        return partitionKey;
    }

    public static Cell createFromByteBuffer(Cell metadata, ByteBuffer cellValue) {
        String cellName = metadata.getCellName();
        boolean isClusterKey = metadata.isClusterKey();
        boolean isKey = metadata.isKey();

        Object o = null;
        if (cellValue != null) {
            o = ((DataType) metadata.getValue()).deserialize(cellValue, ProtocolVersion.V2);

        }

        return Cell.create(cellName, o, isKey, isClusterKey);
    }

    public static Cell createFromEntity(IDeepType e, Field field) {
        DeepField annotation = field.getAnnotation(DeepField.class);
        String cellName = deepFieldName(field);
        Object cellValue = getBeanFieldValue(e, field);
        boolean isClusterKey = annotation.isPartOfClusterKey();
        boolean isKey = annotation.isPartOfPartitionKey();
        return Cell.create(cellName, cellValue, isKey, isClusterKey);

    }

    /**
     * Checks if a token is included in the current split.
     *
     * @param token {@link Token} to be checked.
     * @return true, if the token is included in the interval; false, otherwise.
     */
    public static boolean isTokenIncludedInRange(DeepTokenRange deepTokenRange, Token<Comparable> token) {

        boolean isIncluded = false;

        if (((Comparable) deepTokenRange.getStartTokenAsComparable())
                .compareTo(deepTokenRange.getEndTokenAsComparable()) <= 0) {
            isIncluded = token.token.compareTo(deepTokenRange.getStartTokenAsComparable()) > 0;

            if (isIncluded) {
                isIncluded = token.token.compareTo(deepTokenRange.getEndTokenAsComparable()) <= 0;
            }
        } else {
            isIncluded = token.token.compareTo(deepTokenRange.getStartTokenAsComparable()) > 0;

            if (!isIncluded) {
                isIncluded = token.token.compareTo(deepTokenRange.getEndTokenAsComparable()) <= 0;
            }
        }

        return isIncluded;
    }

}
