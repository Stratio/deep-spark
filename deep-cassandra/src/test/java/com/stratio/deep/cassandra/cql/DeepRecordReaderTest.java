/**
 * 
 */
package com.stratio.deep.cassandra.cql;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.cassandra.dht.Token;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.stratio.deep.cassandra.config.CassandraDeepJobConfig;
import com.stratio.deep.cassandra.filter.value.EqualsInValue;
import com.stratio.deep.commons.rdd.DeepTokenRange;
import com.stratio.deep.commons.utils.Pair;

/**
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ CassandraClientProvider.class })
public class DeepRecordReaderTest {

    private final static int PAGE_SIZE_CONSTANT = 1000;

    private final static String TABLE_NAME_CONSTANT = "TABLENAME";

    private final static String[] INPUT_COLUMNS_CONSTANT = { "col1", "col2", "col3" };

    private final static String LOCALHOST_CONSTANT = "localhost";

    @Mock
    private DeepTokenRange<Long, String> tokenRange;

    @Mock
    private CassandraDeepJobConfig<?> config;

    @Mock
    private Session session;

    @Mock
    private TableMetadata tableMetadata;

    @Mock
    private ColumnMetadata columnMetadata;

    @Mock
    private EqualsInValue equalsInValue;

    @Mock
    private ResultSet resultSet;

    @Test
    public void testEqualsInForDeepRecordReader() {

        // Static stubs
        PowerMockito.mockStatic(CassandraClientProvider.class);

        // Stubbing
        when(config.getPageSize()).thenReturn(PAGE_SIZE_CONSTANT);
        when(config.getTable()).thenReturn(TABLE_NAME_CONSTANT);
        when(config.getInputColumns()).thenReturn(INPUT_COLUMNS_CONSTANT);
        when(config.fetchTableMetadata()).thenReturn(tableMetadata);
        when(config.getEqualsInValue()).thenReturn(equalsInValue);
        when(config.getPartitionerClassName()).thenReturn("org.apache.cassandra.dht.Murmur3Partitioner");

        when(tokenRange.getReplicas()).thenReturn(Arrays.asList(LOCALHOST_CONSTANT));
        when(tokenRange.isTokenIncludedInRange(any(Token.class))).thenReturn(true, false, false, true, true);

        when(CassandraClientProvider.trySessionForLocation(any(String.class), any(CassandraDeepJobConfig.class),
                any(Boolean.class))).thenReturn(Pair.create(session, LOCALHOST_CONSTANT));

        when(tableMetadata.getPartitionKey()).thenReturn(Arrays.asList(columnMetadata, columnMetadata));
        when(tableMetadata.getClusteringColumns()).thenReturn(new ArrayList<ColumnMetadata>());

        when(columnMetadata.getName()).thenReturn("col1", "col2");
        when(columnMetadata.getType()).thenReturn(DataType.bigint());

        when(equalsInValue.getEqualsList()).thenReturn(Arrays.asList(Pair.create("col1", (Serializable) 1L)));
        when(equalsInValue.getInField()).thenReturn("col2");
        when(equalsInValue.getInValues()).thenReturn(
                Arrays.asList((Serializable) 1L, (Serializable) 2L, (Serializable) 3L, (Serializable) 4L,
                        (Serializable) 5L));

        Object[] values = { 1L, Arrays.asList(1L, 4L, 5L) };
        SimpleStatement stmt = new SimpleStatement(
                "SELECT \"col1\",\"col2\",\"col3\" FROM \"TABLENAME\" WHERE col1 = ? AND col2 IN ?", values);

        // TODO Apply the matcher to check the field into the statement
        when(session.execute(any(Statement.class))).thenReturn(resultSet);

        DeepRecordReader recordReader = new DeepRecordReader(config, tokenRange);

        Object[] statementValues = Whitebox.getInternalState(stmt, "values");
        assert (statementValues[0] == values[0]);
        assert (statementValues[1] == values[1]);
    }

    // TODO Move to functional tests
    // @Test
    // public void testDeepRecordReaderInt() {
    //
    // String[] inputColumns = { "id", "id2", "split" };
    // ExtractorConfig<Cells> extractorConfig = new ExtractorConfig<>(Cells.class);
    // extractorConfig.putValue(ExtractorConstants.HOST, "127.0.0.1")
    // .putValue(ExtractorConstants.DATABASE, "demo")
    // .putValue(ExtractorConstants.PORT, 9160)
    // .putValue(ExtractorConstants.COLLECTION, "splits3")
    // .putValue(ExtractorConstants.INPUT_COLUMNS, inputColumns);
    // extractorConfig.setExtractorImplClass(com.stratio.deep.cassandra.extractor.CassandraCellExtractor.class);
    //
    // Serializable[] inValues = { 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L };
    //
    // EqualsInValue equalsInValue = new EqualsInValue();
    // equalsInValue.equalsPair("id", 1L).equalsPair("id2", 2L).inField("split").inValues(Arrays.asList(inValues));
    //
    // extractorConfig.putValue(ExtractorConstants.EQUALS_IN_FILTER, equalsInValue);
    //
    // DeepSparkContext context = new DeepSparkContext("local", "testAppName");
    // DeepJavaRDD<Cells, ?> rdd = (DeepJavaRDD<Cells, ?>) context.createJavaRDD(extractorConfig);
    //
    // long elements = rdd.count();
    // System.out.println("Elements: " + elements);
    // }
}
