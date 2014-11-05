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
import com.stratio.deep.core.entity.MetadataEntity;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class UtilAerospikeTest {

    public static final String KEY_EXAMPLE = "key";

    public static final String AUTHOR = "DANTE ALIGHIERI";
    public static final String TITLE = "THE DIVINE COMEDY";
    public static final String SOURCE = "http://www.gutenberg.org/ebooks/8800";
    public static final Integer PAGES = 649;


    @Test
    public void testGetRecordFromObject() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {


        MetadataEntity metadataEntity = new MetadataEntity();
        metadataEntity.setAuthor(AUTHOR);
        metadataEntity.setTitle(TITLE);
        metadataEntity.setSource(SOURCE);

        BookEntity bookEntity = new BookEntity();
        bookEntity.setId(KEY_EXAMPLE);

        bookEntity.setMetadataEntity(metadataEntity);

        AerospikeRecord record = UtilAerospike.getRecordFromObject(bookEntity);

        // TODO: Test logic

//        BSONObject metadataFromBson = (BSONObject) bson.get("metadata");
//
//        assertEquals(metadataFromBson.get("author"), AUTHOR);
//
//        assertEquals(metadataFromBson.get("title"), TITLE);
//
//        assertEquals(metadataFromBson.get("source"), SOURCE);
//
//
//        List<BSONObject> cantoEntityListFromBson = (List<BSONObject>) bson.get("cantos");
//
//        assertEquals(cantoEntityListFromBson.get(0).get("canto"), CANTO_I);
//
//        assertEquals(cantoEntityListFromBson.get(0).get("text"), TEXT_I);
//
//        assertEquals(cantoEntityListFromBson.get(1).get("canto"), CANTO_II);
//
//        assertEquals(cantoEntityListFromBson.get(1).get("text"), TEXT_II);


    }
}
