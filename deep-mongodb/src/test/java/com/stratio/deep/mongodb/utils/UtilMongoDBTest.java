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

package com.stratio.deep.mongodb.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.stratio.deep.testutils.UnitTest;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.testng.annotations.Test;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.CantoEntity;
import com.stratio.deep.core.entity.MetadataEntity;
import com.stratio.deep.core.entity.WordCount;
import com.stratio.deep.mongodb.testentity.MongoDBTestEntity;

/**
 * Created by rcrespo on 18/06/14.
 */

@Test
@UnitTest
public class UtilMongoDBTest {

    public static final String ID_EXAMPLE = "ID";

    public static final String AUTHOR = "ANTE ALIGHIERI";
    public static final String TITLE = "THE DIVINE COMEDY";
    public static final String SOURCE = "http://www.gutenberg.org/ebooks/8800";

    public static final String CANTO_I = "Canto I";
    public static final String TEXT_I = "text I";

    public static final String CANTO_II = "Canto II";
    public static final String TEXT_II = "text II";

    @Test
    public void testGetBsonFromObject()
            throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException,
            InstantiationException {

        MetadataEntity metadataEntity = new MetadataEntity();
        metadataEntity.setAuthor(AUTHOR);
        metadataEntity.setTitle(TITLE);
        metadataEntity.setSource(SOURCE);

        List<CantoEntity> CantoEntities = new ArrayList<>();

        CantoEntity cantoI = new CantoEntity();
        cantoI.setNumber(CANTO_I);
        cantoI.setText(TEXT_I);

        CantoEntities.add(cantoI);

        CantoEntity cantoII = new CantoEntity();
        cantoII.setNumber(CANTO_II);
        cantoII.setText(TEXT_II);

        CantoEntities.add(cantoII);

        BookEntity bookEntity = new BookEntity();

        bookEntity.setCantoEntities(CantoEntities);

        bookEntity.setMetadataEntity(metadataEntity);

        BSONObject bson = UtilMongoDB.getBsonFromObject(bookEntity);

        BSONObject metadataFromBson = (BSONObject) bson.get("metadata");

        assertEquals(metadataFromBson.get("author"), AUTHOR);

        assertEquals(metadataFromBson.get("title"), TITLE);

        assertEquals(metadataFromBson.get("source"), SOURCE);

        List<BSONObject> cantoEntityListFromBson = (List<BSONObject>) bson.get("cantos");

        assertEquals(cantoEntityListFromBson.get(0).get("canto"), CANTO_I);

        assertEquals(cantoEntityListFromBson.get(0).get("text"), TEXT_I);

        assertEquals(cantoEntityListFromBson.get(1).get("canto"), CANTO_II);

        assertEquals(cantoEntityListFromBson.get(1).get("text"), TEXT_II);

    }

    @Test
    public void testGetObjectFromBson()
            throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException,
            InstantiationException {

        BSONObject bson = createBsonTest();

        BookEntity bookEntity = UtilMongoDB.getObjectFromBson(BookEntity.class, bson);

        MetadataEntity metadata = bookEntity.getMetadataEntity();

        assertEquals(metadata.getAuthor(), AUTHOR);

        assertEquals(metadata.getTitle(), TITLE);

        assertEquals(metadata.getSource(), SOURCE);

        List<CantoEntity> cantoEntityList = bookEntity.getCantoEntities();

        assertEquals(cantoEntityList.get(0).getNumber(), CANTO_I);

        assertEquals(cantoEntityList.get(0).getText(), TEXT_I);

        assertEquals(cantoEntityList.get(1).getNumber(), CANTO_II);

        assertEquals(cantoEntityList.get(1).getText(), TEXT_II);

    }

    @Test
    public void testGetId()
            throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException,
            InstantiationException {

        MongoDBTestEntity commonsBaseTestEntity = new MongoDBTestEntity();

        commonsBaseTestEntity.setId(ID_EXAMPLE);

        assertEquals(UtilMongoDB.getId(commonsBaseTestEntity), ID_EXAMPLE);

        WordCount wordCount = new WordCount();

        assertNull(UtilMongoDB.getId(wordCount));
    }

    private BSONObject createBsonTest() {
        BSONObject bson = new BasicBSONObject();

        BSONObject metadata = new BasicBSONObject();
        metadata.put("author", AUTHOR);
        metadata.put("title", TITLE);
        metadata.put("source", SOURCE);

        BSONObject cantoI = new BasicBSONObject();

        cantoI.put("canto", CANTO_I);
        cantoI.put("text", TEXT_I);

        BSONObject cantoII = new BasicBSONObject();
        cantoII.put("canto", CANTO_II);
        cantoII.put("text", TEXT_II);

        List<BSONObject> cantosList = new ArrayList<>();
        cantosList.add(cantoI);
        cantosList.add(cantoII);

        bson.put("metadata", metadata);
        bson.put("cantos", cantosList);

        return bson;
    }

    @Test
    public void testGetCellFromBson()
            throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException,
            InstantiationException {

        BSONObject bson = createBsonTest();

        Cells cells = UtilMongoDB.getCellFromBson(bson, "book");

        // Check metadata Object

        Map<String, Object> mapMetadata = (Map<String, Object>) bson.get("metadata");

        assertEquals(mapMetadata.get("author"),
                ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("author").getCellValue());
        assertEquals(mapMetadata.get("title"),
                ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("title").getCellValue());
        assertEquals(mapMetadata.get("source"),
                ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("source").getCellValue());

        // Check list Oject

        List<Cells> list = (List<Cells>) cells.getCellByName("cantos").getCellValue();

        List<Map<String, Object>> mapCantos = (List<Map<String, Object>>) bson.get("cantos");

        assertEquals(mapCantos.get(0).get("canto"), list.get(0).getCellByName("canto").getCellValue());
        assertEquals(mapCantos.get(0).get("text"), list.get(0).getCellByName("text").getCellValue());

        assertEquals(mapCantos.get(1).get("canto"), list.get(1).getCellByName("canto").getCellValue());
        assertEquals(mapCantos.get(1).get("text"), list.get(1).getCellByName("text").getCellValue());

    }

    @Test
    public void testGetBsonFromCell()
            throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException,
            InstantiationException {

        //Create Medataba Object

        Cell authorCell = Cell.create("author", "ANTE ALIGHIERI");
        Cell titleCell = Cell.create("title", "THE DIVINE COMEDY");
        Cell sourceCell = Cell.create("source", "http://www.gutenberg.org/ebooks/8800");

        Cells metadata = new Cells();

        metadata.add(authorCell);
        metadata.add(titleCell);
        metadata.add(sourceCell);

        //Create Cantos Object

        List<Cells> cantos = new ArrayList<>();

        Cells cantoI = new Cells();

        cantoI.add(Cell.create("canto", "Canto I"));
        cantoI.add(Cell.create("text", "text I"));

        Cells cantoII = new Cells();

        cantoII.add(Cell.create("canto", "Canto II"));
        cantoII.add(Cell.create("text", "text II"));

        cantos.add(cantoI);
        cantos.add(cantoII);

        // Put all together

        Cells cells = new Cells();

        cells.add(Cell.create("metadata", metadata));
        cells.add(Cell.create("cantos", cantos));

        BSONObject bson = UtilMongoDB.getBsonFromCell(cells);

        // Check metadata Object

        Map<String, Object> mapMetadata = (Map<String, Object>) bson.get("metadata");

        assertEquals(mapMetadata.get("author"),
                ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("author").getCellValue());
        assertEquals(mapMetadata.get("title"),
                ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("title").getCellValue());
        assertEquals(mapMetadata.get("source"),
                ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("source").getCellValue());

        // Check list Oject

        List<Cells> list = (List<Cells>) cells.getCellByName("cantos").getCellValue();

        List<Map<String, Object>> mapCantos = (List<Map<String, Object>>) bson.get("cantos");

        assertEquals(mapCantos.get(0).get("canto"), list.get(0).getCellByName("canto").getCellValue());
        assertEquals(mapCantos.get(0).get("text"), list.get(0).getCellByName("text").getCellValue());

        assertEquals(mapCantos.get(1).get("canto"), list.get(1).getCellByName("canto").getCellValue());
        assertEquals(mapCantos.get(1).get("text"), list.get(1).getCellByName("text").getCellValue());

    }

    @Test(expectedExceptions = InvocationTargetException.class)
    public void testConstructorIsPrivate()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<UtilMongoDB> constructor = UtilMongoDB.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }

}
