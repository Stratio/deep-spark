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
import com.stratio.deep.aerospike.reader.AerospikeReader;
import com.stratio.deep.aerospike.writer.AerospikeWriter;
import com.stratio.deep.commons.entity.Cells;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

/**
 * AerospikeNativeCellExtractor unit tests.
 */
@Test(groups = {"UnitTests"})
public class AerospikeNativeCellExtractorTest {

    private AerospikeReader aerospikeReader = PowerMockito.mock(AerospikeReader.class);

    private AerospikeWriter aerospikeWriter = PowerMockito.mock(AerospikeWriter.class);

    @Test
    public void testSaveRdd() {
        AerospikeNativeCellExtractor extractor = createAerospikeNativeExtractor();
        extractor.saveRDD(new Cells());
        verify(aerospikeWriter, times(1)).save(any(Record.class));
    }

    private AerospikeNativeCellExtractor createAerospikeNativeExtractor() {
        AerospikeNativeCellExtractor extractor = new AerospikeNativeCellExtractor(Cells.class);
        Whitebox.setInternalState(extractor, "reader", aerospikeReader);
        Whitebox.setInternalState(extractor, "writer", aerospikeWriter);
        return extractor;
    }
}
