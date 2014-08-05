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

package com.stratio.deep.testutils;

import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.ESCell;
import com.stratio.deep.testentity.*;
import com.stratio.deep.utils.UtilES;

import org.json.simple.JSONObject;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    public void testGetjsonFromObject() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {


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

        JSONObject json = UtilES.getJsonFromObject(bookEntity);


        JSONObject metadataFromjson = (JSONObject) json.get("metadata");

        assertEquals(metadataFromjson.get("author"), AUTHOR);

        assertEquals(metadataFromjson.get("title"), TITLE);

        assertEquals(metadataFromjson.get("source"), SOURCE);


        List<JSONObject> cantoEntityListFromjson = (List<JSONObject>) json.get("cantos");

        assertEquals(cantoEntityListFromjson.get(0).get("canto"), CANTO_I);

        assertEquals(cantoEntityListFromjson.get(0).get("text"), TEXT_I);

        assertEquals(cantoEntityListFromjson.get(1).get("canto"), CANTO_II);

        assertEquals(cantoEntityListFromjson.get(1).get("text"), TEXT_II);


    }

    @Test
    public void testGetObjectFromjson() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {

//        JSONObject json = createjsonTest();
//
//        BookEntity bookEntity = UtilES.getObjectFromJson(BookEntity.class, json);
//
//
//        MetadataEntity metadata = bookEntity.getMetadataEntity();
//
//        assertEquals(metadata.getAuthor(), AUTHOR);
//
//        assertEquals(metadata.getTitle(), TITLE);
//
//        assertEquals(metadata.getSource(), SOURCE);
//
//
//        List<CantoEntity> cantoEntityList = bookEntity.getCantoEntities();
//
//        assertEquals(cantoEntityList.get(0).getNumber(), CANTO_I);
//
//        assertEquals(cantoEntityList.get(0).getText(), TEXT_I);
//
//        assertEquals(cantoEntityList.get(1).getNumber(), CANTO_II);
//
//        assertEquals(cantoEntityList.get(1).getText(), TEXT_II);

    }

//    @Test
//    public void testGetId() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {
//
//        ESTestEntity commonsBaseTestEntity = new ESTestEntity();
//
//        commonsBaseTestEntity.setId(ID_EXAMPLE);
//
//
//        assertEquals(UtilES.getId(commonsBaseTestEntity), ID_EXAMPLE);
//
//
//        WordCount wordCount = new WordCount();
//
//        assertNull(UtilES.getId(wordCount));
//    }


    private JSONObject createjsonTest() {
        JSONObject json = new JSONObject();

        JSONObject metadata = new JSONObject();
        metadata.put("author", AUTHOR);
        metadata.put("title", TITLE);
        metadata.put("source", SOURCE);


        JSONObject cantoI = new JSONObject();

        cantoI.put("canto", CANTO_I);
        cantoI.put("text", TEXT_I);

        JSONObject cantoII = new JSONObject();
        cantoII.put("canto", CANTO_II);
        cantoII.put("text", TEXT_II);


        List<JSONObject> cantosList = new ArrayList<>();
        cantosList.add(cantoI);
        cantosList.add(cantoII);

        json.put("metadata", metadata);
        json.put("cantos", cantosList);

        return json;
    }

    @Test
    public void testGetCellFromjson() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {

//        JSONObject json = createjsonTest();
//
//        Cells cells = UtilES.getCellFromJson(json);
//
//
//        // Check metadata Object
//
//
//        Map<String, Object> mapMetadata = (Map<String, Object>) json.get("metadata");
//
//        assertEquals(mapMetadata.get("author"), ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("author").getCellValue());
//        assertEquals(mapMetadata.get("title"), ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("title").getCellValue());
//        assertEquals(mapMetadata.get("source"), ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("source").getCellValue());
//
//
//        // Check list Oject
//
//        List<Cells> list = (List<Cells>) cells.getCellByName("cantos").getCellValue();
//
//        List<Map<String, Object>> mapCantos = (List<Map<String, Object>>) json.get("cantos");
//
//
//        assertEquals(mapCantos.get(0).get("canto"), list.get(0).getCellByName("canto").getCellValue());
//        assertEquals(mapCantos.get(0).get("text"), list.get(0).getCellByName("text").getCellValue());
//
//        assertEquals(mapCantos.get(1).get("canto"), list.get(1).getCellByName("canto").getCellValue());
//        assertEquals(mapCantos.get(1).get("text"), list.get(1).getCellByName("text").getCellValue());


    }

    @Test
    public void testGetjsonFromCell() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {


        //Create Medataba Object

        Cell authorCell = ESCell.create("author", "ANTE ALIGHIERI");
        Cell titleCell = ESCell.create("title", "THE DIVINE COMEDY");
        Cell sourceCell = ESCell.create("source", "http://www.gutenberg.org/ebooks/8800");

        Cells metadata = new Cells();

        metadata.add(authorCell);
        metadata.add(titleCell);
        metadata.add(sourceCell);


        //Create Cantos Object

        List<Cells> cantos = new ArrayList<>();

        Cells cantoI = new Cells();

        cantoI.add(ESCell.create("canto", "Canto I"));
        cantoI.add(ESCell.create("text", "text I"));

        Cells cantoII = new Cells();

        cantoII.add(ESCell.create("canto", "Canto II"));
        cantoII.add(ESCell.create("text", "text II"));

        cantos.add(cantoI);
        cantos.add(cantoII);


        // Put all together

        Cells cells = new Cells();

        cells.add(ESCell.create("metadata", metadata));
        cells.add(ESCell.create("cantos", cantos));


        JSONObject json = UtilES.getJsonFromCell(cells);


        // Check metadata Object

        Map<String, Object> mapMetadata = (Map<String, Object>) json.get("metadata");

        assertEquals(mapMetadata.get("author"), ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("author").getCellValue());
        assertEquals(mapMetadata.get("title"), ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("title").getCellValue());
        assertEquals(mapMetadata.get("source"), ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("source").getCellValue());


        // Check list Oject

        List<Cells> list = (List<Cells>) cells.getCellByName("cantos").getCellValue();

        List<Map<String, Object>> mapCantos = (List<Map<String, Object>>) json.get("cantos");


        assertEquals(mapCantos.get(0).get("canto"), list.get(0).getCellByName("canto").getCellValue());
        assertEquals(mapCantos.get(0).get("text"), list.get(0).getCellByName("text").getCellValue());

        assertEquals(mapCantos.get(1).get("canto"), list.get(1).getCellByName("canto").getCellValue());
        assertEquals(mapCantos.get(1).get("text"), list.get(1).getCellByName("text").getCellValue());


    }


    @Test(expectedExceptions = InvocationTargetException.class)
    public void testConstructorIsPrivate() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<UtilES> constructor = UtilES.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }


}
