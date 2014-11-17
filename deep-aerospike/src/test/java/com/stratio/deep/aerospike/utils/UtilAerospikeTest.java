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
package com.stratio.deep.aerospike.utils;

import com.aerospike.hadoop.mapreduce.AerospikeRecord;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.CantoEntity;
import com.stratio.deep.core.entity.MessageTestEntity;
import com.stratio.deep.core.entity.MetadataEntity;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

@Test
public class UtilAerospikeTest {

    @Test
    public void testGetRecordFromObject() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {

        MessageTestEntity messageEntity = new MessageTestEntity();
        messageEntity.setId("3");
        messageEntity.setMessage("Test message");

        AerospikeRecord record = UtilAerospike.getRecordFromObject(messageEntity);
        Map<String, Object> bins = record.bins;

        assertEquals(bins.get("id"), messageEntity.getId());
        assertEquals(bins.get("message"), messageEntity.getMessage());

    }
}
