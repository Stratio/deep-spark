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

import static org.testng.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.utils.Pair;
import org.testng.annotations.Test;

import com.aerospike.client.Record;
import com.aerospike.hadoop.mapreduce.AerospikeRecord;
import com.stratio.deep.aerospike.config.AerospikeDeepJobConfig;
import com.stratio.deep.core.entity.MessageTestEntity;

@Test(groups = { "UnitTests" })
public class UtilAerospikeTest {

    @Test
    public void testGetRecordFromObject()
            throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException,
            InstantiationException {

        MessageTestEntity messageEntity = new MessageTestEntity();
        messageEntity.setId("3");
        messageEntity.setMessage("Test message");

        Pair<Object, AerospikeRecord> record = UtilAerospike.getAerospikeRecordFromObject(messageEntity);
        Map<String, Object> bins = record.right.bins;

        Object key = record.left;
        assertEquals(messageEntity.getId().toString(), key.toString());

        assertEquals(bins.get("id"), messageEntity.getId());
        assertEquals(bins.get("message"), messageEntity.getMessage());

    }

    @Test
    public void testGetObjectFromRecord()
            throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException,
            InstantiationException {
        Map<String, Object> bins = new HashMap<>();
        bins.put("id", "3");
        bins.put("message", "Message");
        bins.put("number", 3L);
        Record data = new Record(bins, 0, 0);
        AerospikeRecord record = new AerospikeRecord(data);
        MessageTestEntity messageEntity = UtilAerospike.getObjectFromAerospikeRecord(MessageTestEntity.class, record,
                new AerospikeDeepJobConfig(MessageTestEntity.class));

        assertEquals(messageEntity.getId(), bins.get("id"));
        assertEquals(messageEntity.getMessage(), bins.get("message"));

    }

    @Test
    public void testGetCellFromRecord() throws InstantiationException, IllegalAccessException, InvocationTargetException {
        Cells cells = new Cells();
        cells.add(Cell.create("id", "1", true));
        cells.add(Cell.create("message", "message", false));

        Pair<Object, AerospikeRecord> record = UtilAerospike.getAerospikeRecordFromCell(cells);
        Map<String, Object> bins = record.right.bins;

        Object key = record.left;
        assertEquals(cells.getString("id"), key.toString());

        assertEquals(cells.size(), bins.size());
        assertEquals(cells.getString("id"), bins.get("id"));
        assertEquals(cells.getString("message"), bins.get("message"));
    }

    public void testGetRecordFromCell() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException,
            InstantiationException {
        Map<String, Object> bins = new HashMap<>();
        bins.put("id", "3");
        bins.put("message", "Message");
        Record data = new Record(bins, 0, 0);
        AerospikeRecord record = new AerospikeRecord(data);
        Cells cells = UtilAerospike.getCellFromAerospikeRecord(null, record, new AerospikeDeepJobConfig(Cells.class));

        assertEquals(bins.size(), cells.size());
        assertEquals(bins.get("id"), cells.getString("id"));
        assertEquals(bins.get("message"), cells.getString("message"));
    }

}
