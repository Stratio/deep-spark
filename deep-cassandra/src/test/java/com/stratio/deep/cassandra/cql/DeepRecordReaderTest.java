/**
 * 
 */
package com.stratio.deep.cassandra.cql;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
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
        when(tokenRange.getStartTokenAsComparable()).thenReturn(-8000000000000000000L, -7000000000000000000L,
                2200000000000000000L, 2300000000000000000L, 2600000000000000000L);
        when(tokenRange.getEndTokenAsComparable()).thenReturn(-7000000000000000000L, 2200000000000000000L,
                2300000000000000000L, 2600000000000000000L, -8000000000000000000L);

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

        Object[] values = { 1L, Arrays.asList(1L, 4L) };
        SimpleStatement stmt = new SimpleStatement(
                "SELECT \"col1\",\"col2\",\"col3\" FROM \"TABLENAME\" WHERE col1 = ? AND col2 IN ?", values);
        when(session.execute(any(Statement.class))).thenReturn(resultSet);

        DeepRecordReader recordReader = new DeepRecordReader(config, tokenRange);

        // TODO How do we check two statements are the same?
        verify(session, times(1)).execute(Matchers.argThat(new StatementMatcher(stmt)));
    }

    class StatementMatcher extends BaseMatcher<SimpleStatement> {

        private final SimpleStatement expectedStmt;

        public StatementMatcher(SimpleStatement expectedStmt) {
            this.expectedStmt = expectedStmt;
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean matches(Object obj) {
            if (obj != null && obj instanceof SimpleStatement) {
                SimpleStatement receivedStmt = (SimpleStatement) obj;
                Object[] expectedValues = Whitebox.getInternalState(expectedStmt, "values");
                Object[] receivedValues = Whitebox.getInternalState(receivedStmt, "values");

                return matchValues(expectedValues, receivedValues);
            }
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
         */
        @Override
        public void describeTo(Description description) {
            description.appendText("Matches a class or subclass");
        }

        private boolean matchValues(Object[] expectedValues, Object[] receivedValues) {

            boolean match = true;
            int pointer = 0;
            while (pointer < expectedValues.length && match) {
                if (expectedValues[pointer] instanceof Long) {
                    Long expectedValue = (Long) expectedValues[pointer];
                    Long receivedValue = (Long) receivedValues[pointer];
                    match = expectedValue.equals(receivedValue);
                } else if (expectedValues[pointer] instanceof List) {
                    List<Long> expectedValue = (List<Long>) expectedValues[pointer];
                    List<Long> receivedValue = (List<Long>) receivedValues[pointer];
                    match = expectedValue.equals(receivedValue);
                } else {
                    match = expectedValues[pointer].equals(receivedValues[pointer]);
                }

                pointer++;
            }

            return match;
        }
    }
}
