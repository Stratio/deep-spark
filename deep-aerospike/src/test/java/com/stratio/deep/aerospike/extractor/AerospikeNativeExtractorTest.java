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
package com.stratio.deep.aerospike.extractor;

import com.aerospike.client.Record;
import com.stratio.deep.aerospike.config.AerospikeDeepJobConfig;
import com.stratio.deep.aerospike.reader.AerospikeReader;
import com.stratio.deep.aerospike.writer.AerospikeWriter;
import com.stratio.deep.commons.entity.Cells;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.mockito.Mockito.*;

/**
 * AerospikeNativeExtractor unit tests.
 */
@Test(groups = {"UnitTests"})
public class AerospikeNativeExtractorTest {

    private static final String HOST = "localhost";
    private static final int PORT = 3000;
    private static final String SET = "set";

    private AerospikeReader aerospikeReader = PowerMockito.mock(AerospikeReader.class);

    private AerospikeWriter aerospikeWriter = PowerMockito.mock(AerospikeWriter.class);

    @Test
    public void testPartitions() {

    }

    @Test
    public void testHasNext() {
        AerospikeNativeExtractor extractor = createAerospikeNativeExtractor();
        extractor.hasNext();
        verify(aerospikeReader, times(1)).hasNext();
    }

    @Test
    public void testSaveRdd() {
        AerospikeNativeExtractor extractor = createAerospikeNativeExtractor();
        doReturn(new Record(new HashMap<String, Object>(), 0, 0)).when(extractor).transformElement(any(Object.class));
        extractor.saveRDD(new Object());
        verify(aerospikeWriter, times(1)).save(any(Record.class));
    }

    @Test
    public void testClose() {
        AerospikeNativeExtractor extractor = createAerospikeNativeExtractor();
        extractor.close();
        verify(aerospikeReader, times(1)).close();
        verify(aerospikeWriter, times(1)).close();
    }

    private AerospikeNativeExtractor createAerospikeNativeExtractor() {
        AerospikeNativeExtractor aerospikeNativeExtractor = PowerMockito.mock(AerospikeNativeExtractor.class, Mockito.CALLS_REAL_METHODS);
        Whitebox.setInternalState(aerospikeNativeExtractor, "aerospikeDeepJobConfig", new AerospikeDeepJobConfig<>(Cells.class));
        Whitebox.setInternalState(aerospikeNativeExtractor, "reader", aerospikeReader);
        Whitebox.setInternalState(aerospikeNativeExtractor, "writer", aerospikeWriter);
        return aerospikeNativeExtractor;
    }


}
