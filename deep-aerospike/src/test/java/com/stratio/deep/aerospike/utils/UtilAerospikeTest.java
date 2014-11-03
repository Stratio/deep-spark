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

/**
 * Created by mariomgal on 01/11/14.
 */
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
