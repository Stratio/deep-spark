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

package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.jdbc.config.JdbcConfigFactory;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.reader.JdbcReader;
import com.stratio.deep.jdbc.writer.JdbcWriter;
import org.apache.spark.Partition;
import org.apache.spark.rdd.JdbcPartition;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

/**
 * Created by mariomgal on 12/12/14.
 */
@Test(groups = { "UnitTests" })
public class JdbcNativeExtractorTest {

    private static final String HOST = "localhost";
    private static final int PORT = 3306;
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final int LOWER_BOUND = 0;
    private static final int UPPER_BOUND = 19;
    private static final int NUM_PARTITIONS = 2;

    private JdbcReader jdbcReader = PowerMockito.mock(JdbcReader.class);

    private JdbcWriter jdbcWriter = PowerMockito.mock(JdbcWriter.class);

    @Test
    public void testPartitions() {
        JdbcNativeExtractor extractor = createJdbcNativeExtractor();

        Partition [] partitions = extractor.getPartitions(createJdbcDeepJobConfig());
        assertEquals(partitions.length, NUM_PARTITIONS);

        JdbcPartition partition0 = (JdbcPartition)partitions[0];
        assertEquals(partition0.index(), 0);
        assertEquals(partition0.lower(), 0);
        assertEquals(partition0.upper(), 9);

        JdbcPartition partition1 = (JdbcPartition)partitions[1];
        assertEquals(partition1.index(), 1);
        assertEquals(partition1.lower(), 10);
        assertEquals(partition1.upper(), 19);
    }

    @Test
    public void testHasNext() throws Exception {
        JdbcNativeExtractor extractor = createJdbcNativeExtractor();
        extractor.hasNext();
        verify(jdbcReader, times(1)).hasNext();
    }

    @Test
    public void testSaveRdd() throws Exception {
        JdbcNativeExtractor extractor = createJdbcNativeExtractor();
        doReturn(new HashMap()).when(extractor).transformElement(any(Object.class));
        extractor.saveRDD(new Object());
        verify(jdbcWriter, times(1)).save(any(Map.class));
    }

    @Test
    public void testClose() throws Exception {
        JdbcNativeExtractor extractor = createJdbcNativeExtractor();
        extractor.close();
        verify(jdbcWriter, times(1)).close();
        verify(jdbcReader, times(1)).close();
    }

    private JdbcNativeExtractor createJdbcNativeExtractor() {
        JdbcNativeExtractor extractor = PowerMockito.mock(JdbcNativeExtractor.class, Mockito.CALLS_REAL_METHODS);
        Whitebox.setInternalState(extractor, "jdbcDeepJobConfig", new JdbcDeepJobConfig<>(Cells.class));
        Whitebox.setInternalState(extractor, "jdbcReader", jdbcReader);
        Whitebox.setInternalState(extractor, "jdbcWriter", jdbcWriter);
        return extractor;
    }

    private JdbcDeepJobConfig createJdbcDeepJobConfig() {
        JdbcDeepJobConfig result = JdbcConfigFactory.createJdbc();
        result.lowerBound(LOWER_BOUND).upperBound(UPPER_BOUND).numPartitions(NUM_PARTITIONS);
        ExtractorConfig config = new ExtractorConfig();
        config.putValue(ExtractorConstants.HOST, HOST);
        config.putValue(ExtractorConstants.PORT, PORT);
        config.putValue(ExtractorConstants.JDBC_DRIVER_CLASS, DRIVER_CLASS);
        config.putValue(ExtractorConstants.CATALOG, DATABASE);
        config.putValue(ExtractorConstants.TABLE, TABLE);
        result.initialize(config);
        return result;
    }
}
