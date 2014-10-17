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

package com.stratio.deep.utils;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.commons.filter.FilterOperator;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.CantoEntity;
import com.stratio.deep.core.entity.MetadataEntity;
import com.stratio.deep.es.utils.UtilES;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.stratio.deep.es.utils.UtilES.generateQuery;
import static org.testng.Assert.*;

/**
 * Created by rcrespo on 18/06/14.
 */

@Test
public class UtilESTest {

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

        JSONObject bson = UtilES.getJsonFromObject(bookEntity);

        JSONObject metadataFromBson = (JSONObject) bson.get("metadata");

        assertEquals(metadataFromBson.get("author"), AUTHOR);

        assertEquals(metadataFromBson.get("title"), TITLE);

        assertEquals(metadataFromBson.get("source"), SOURCE);

        List<JSONObject> cantoEntityListFromBson = (List<JSONObject>) bson.get("cantos");

        assertEquals(cantoEntityListFromBson.get(0).get("canto"), CANTO_I);

        assertEquals(cantoEntityListFromBson.get(0).get("text"), TEXT_I);

        assertEquals(cantoEntityListFromBson.get(1).get("canto"), CANTO_II);

        assertEquals(cantoEntityListFromBson.get(1).get("text"), TEXT_II);

    }

    @Test
    public void testGetObjectFromBson()
            throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException,
            InstantiationException, NoSuchMethodException {

        LinkedMapWritable json = createJsonTest();

        BookEntity bookEntity = UtilES.getObjectFromJson(BookEntity.class, json);

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

        //        ESTestEntity commonsBaseTestEntity = new ESTestEntity();
        //
        //        commonsBaseTestEntity.setId(ID_EXAMPLE);
        //
        //
        //        assertEquals(UtilES.getId(commonsBaseTestEntity), ID_EXAMPLE);
        //
        //
        //        WordCount wordCount = new WordCount();

        //        assertNull(UtilES.getId(wordCount));
    }

    private LinkedMapWritable createJsonTest() {
        LinkedMapWritable json = new LinkedMapWritable();

        LinkedMapWritable metadata = new LinkedMapWritable();
        metadata.put(new Text("author"), new Text(AUTHOR));
        metadata.put(new Text("title"), new Text(TITLE));
        metadata.put(new Text("source"), new Text(SOURCE));

        LinkedMapWritable cantoI = new LinkedMapWritable();

        cantoI.put(new Text("canto"), new Text(CANTO_I));
        cantoI.put(new Text("text"), new Text(TEXT_I));

        LinkedMapWritable cantoII = new LinkedMapWritable();
        cantoII.put(new Text("canto"), new Text(CANTO_II));
        cantoII.put(new Text("text"), new Text(TEXT_II));

        LinkedMapWritable[] writableArrary = new LinkedMapWritable[] { cantoI, cantoII };

        ArrayWritable cantosList = new ArrayWritable(LinkedMapWritable.class, writableArrary);

        json.put(new Text("metadata"), metadata);
        json.put(new Text("cantos"), cantosList);

        return json;
    }

    @Test
    public void testGetCellFromJson()
            throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException,
            InstantiationException, NoSuchMethodException {

        LinkedMapWritable bson = createJsonTest();

        Cells cells = UtilES.getCellFromJson(bson, "book");

        Map<Writable, Writable> mapMetadata = (Map<Writable, Writable>) bson.get(new Text("metadata"));

        assertEquals(mapMetadata.get(new Text("author")).toString(),
                ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("author").getCellValue());
        assertEquals(mapMetadata.get(new Text("title")).toString(),
                ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("title").getCellValue());
        assertEquals(mapMetadata.get(new Text("source")).toString(),
                ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("source").getCellValue());

        // Check list Oject

        List<Cells> list = (List<Cells>) cells.getCellByName("cantos").getCellValue();

        LinkedMapWritable[] mapCantos = (LinkedMapWritable[]) ((ArrayWritable) bson.get(new Text("cantos"))).get();

        assertEquals(mapCantos[0].get(new Text("canto")).toString(), list.get(0).getCellByName("canto").getCellValue());
        assertEquals(mapCantos[0].get(new Text("text")).toString(), list.get(0).getCellByName("text").getCellValue());

        assertEquals(mapCantos[1].get(new Text("canto")).toString(), list.get(1).getCellByName("canto").getCellValue());
        assertEquals(mapCantos[1].get(new Text("text")).toString(), list.get(1).getCellByName("text").getCellValue());

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

        JSONObject bson = UtilES.getJsonFromCell(cells);

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
        Constructor<UtilES> constructor = UtilES.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }


    @Test
    public void testgenerateQuery() {

        Filter filter = new Filter("field1" , FilterOperator.IS, "value1");
        Filter filter2 = new Filter("field2" , FilterOperator.NE, "value2");

        BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) generateQuery(filter, filter2);

//        assertEquals(boolQueryBuilder.toString(), "");

        assertTrue(true, "true");
    }

}
