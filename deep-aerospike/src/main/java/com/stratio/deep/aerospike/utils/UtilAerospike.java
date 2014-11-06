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

import com.aerospike.client.Record;
import com.aerospike.hadoop.mapreduce.AerospikeKey;
import com.aerospike.hadoop.mapreduce.AerospikeRecord;
import com.stratio.deep.aerospike.config.AerospikeDeepJobConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.utils.AnnotationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Several utilities to work used in the Spark <=> Aerospike integration.
 */
public class UtilAerospike {

    private static final Logger LOG = LoggerFactory.getLogger(UtilAerospike.class);

    /**
     * Private default constructor.
     */
    private UtilAerospike() {
        throw new UnsupportedOperationException();
    }

    /**
     * converts from AerospikeRecord to an entity class with deep's anotations
     *
     * @param classEntity the entity name.
     * @param aerospikeRecord  the instance of the AerospikeRecord to convert.
     * @param <T>         return type.
     * @return the provided aerospikeRecord converted to an instance of T.
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws java.lang.reflect.InvocationTargetException
     */
    public static <T> T getObjectFromRecord(Class<T> classEntity, AerospikeRecord aerospikeRecord) throws IllegalAccessException, InstantiationException, InvocationTargetException {
        T t = classEntity.newInstance();

        Field[] fields = AnnotationUtils.filterDeepFields(classEntity);

        Object insert = null;

        // TODO: Record -> Object conversion logic

        for (Field field : fields) {

        }

        return t;
    }

    /**
     * converts from an entity class with deep's anotations to AerospikeRecord.
     *
     * @param t   an instance of an object of type T to convert to AerospikeRecord.
     * @param <T> the type of the object to convert.
     * @return the provided object converted to AerospikeRecord.
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static <T> AerospikeRecord getRecordFromObject(T t) throws IllegalAccessException, InstantiationException, InvocationTargetException {
        Field[] fields = AnnotationUtils.filterDeepFields(t.getClass());

        AerospikeRecord record = new AerospikeRecord();

        // TODO: Object -> Record conversion logic

        return record;
    }

    /**
     * converts from AerospikeRecord to cell class with deep's anotations
     *
     * @param aerospikeRecord
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static Cells getCellFromRecord(AerospikeKey key, AerospikeRecord aerospikeRecord, AerospikeDeepJobConfig aerospikeConfig) throws IllegalAccessException,
            InstantiationException, InvocationTargetException {

        String namespace = aerospikeConfig.getNamespace();
        String setName = aerospikeConfig.getSet();
        String [] inputColumns = aerospikeConfig.getInputColumns();
        Tuple2<String, Object> equalsFilter = aerospikeConfig.getEqualsFilter();
        String equalsFilterBin = equalsFilter!=null ? equalsFilter._1():null;
        Object equalsFilterValue = equalsFilter!=null ? equalsFilter._2():null;

        Cells cells = setName!= null ?new Cells(setName): new Cells();

        Map<String, Object> map = aerospikeRecord.bins;
        if(inputColumns != null) {
            if(equalsFilter == null || checkEqualityFilter(map, equalsFilterBin, equalsFilterValue)) {
                for (int i = 0; i < inputColumns.length; i++) {
                    String binName = inputColumns[i];
                    if (map.containsKey(binName)) {
                        Cell cell = Cell.create(binName, map.get(binName));
                        cells.add(cell);
                    } else {
                        throw new InvocationTargetException(new Exception("There is no [" + binName + "] on aerospike [" + namespace + "." + setName + "] set"));
                    }
                }
            }
        } else {
            if(equalsFilter == null || checkEqualityFilter(map, equalsFilterBin, equalsFilterValue)) {
                for (Map.Entry<String, Object> bin : map.entrySet()) {
                    Cell cell = Cell.create(bin.getKey(), bin.getValue());
                    cells.add(cell);
                }
            }
        }

        return cells;
    }

    private static boolean checkEqualityFilter(Map<String, Object> bins, String binName, Object binValue) {
        return bins.containsKey(binName) && bins.get(binName).equals(binValue);
    }

    /**
     * converts from and entity class with deep's anotations to BsonObject
     *
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static AerospikeRecord getRecordFromCell(Cells cells) throws IllegalAccessException, InstantiationException, InvocationTargetException {
        AerospikeRecord result = new AerospikeRecord();
        Map<String, Object> bins = new HashMap<>();
        for(Cell cell:cells.getCells()) {
            bins.put(cell.getCellName(), cell.getValue());
        }
        Record record = new Record(bins, null, 0, 0);  // Expiration time = 0, defaults to namespace configuration ("default-ttl")
        result = new AerospikeRecord(record);
        return result;
    }


}
