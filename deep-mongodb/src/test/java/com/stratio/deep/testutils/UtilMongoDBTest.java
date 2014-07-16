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

import com.stratio.deep.entity.Cells;
import com.stratio.deep.testentity.CommonsTestEntity;
import com.stratio.deep.testentity.MongoDBTestEntity;
import com.stratio.deep.utils.UtilMongoDB;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Created by rcrespo on 18/06/14.
 */

@Test
public class UtilMongoDBTest {


    private static final String URL_EXAMPLE = "http:www.example.com";

    private static final String ID_EXAMPLE = "ID";

    @Test
    public void testGetBsonFromObject() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {
        CommonsTestEntity commonsTestEntity = new CommonsTestEntity();

        commonsTestEntity.setUrl(URL_EXAMPLE);


        BSONObject bson = UtilMongoDB.getBsonFromObject(commonsTestEntity);


        assertEquals(bson.get("url"), URL_EXAMPLE);


    }

    @Test
    public void testGetObjectFromBson() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {

        BSONObject bson = new BasicBSONObject();

        bson.put("url", URL_EXAMPLE);

        CommonsTestEntity commonsTestEntity = UtilMongoDB.getObjectFromBson(CommonsTestEntity.class, bson);


        assertEquals(commonsTestEntity.getUrl(), URL_EXAMPLE);
    }

    @Test
    public void testGetId() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {

        MongoDBTestEntity commonsBaseTestEntity = new MongoDBTestEntity();

        commonsBaseTestEntity.setId(ID_EXAMPLE);


        assertEquals(UtilMongoDB.getId(commonsBaseTestEntity), ID_EXAMPLE);
    }


    @Test
    public void testGetCellFromBson() throws UnknownHostException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {
        BSONObject bson = new BasicBSONObject();

        BSONObject metadata = new BasicBSONObject();
        metadata.put("author", "ANTE ALIGHIERI");
        metadata.put("title", "THE DIVINE COMEDY");
        metadata.put("source", "http://www.gutenberg.org/ebooks/8800");


        BSONObject cantoI = new BasicBSONObject();

        cantoI.put("canto", "Canto I");
        cantoI.put("text", "text I");

        BSONObject cantoII = new BasicBSONObject();
        cantoII.put("canto", "Canto II");
        cantoII.put("text", "text II");


        List<BSONObject> cantosList = new ArrayList<>();
        cantosList.add(cantoI);
        cantosList.add(cantoII);

        bson.put("metadata", metadata);
        bson.put("cantos", cantosList);

        Cells cells = UtilMongoDB.getCellFromBson(bson);


        // Check metadata Object


        Map<String, Object> mapMetadata = (Map<String, Object>) bson.get("metadata");

        assertEquals(mapMetadata.get("author"), ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("author").getCellValue());
        assertEquals(mapMetadata.get("title"), ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("title").getCellValue());
        assertEquals(mapMetadata.get("source"), ((Cells) cells.getCellByName("metadata").getCellValue()).getCellByName("source").getCellValue());


        // Check list Oject

        List<Cells> list = (List<Cells>) cells.getCellByName("cantos").getCellValue();

        List<Map<String, Object>> mapCantos = (List<Map<String, Object>>) bson.get("cantos");


        assertEquals(mapCantos.get(0).get("canto"), list.get(0).getCellByName("canto").getCellValue());
        assertEquals(mapCantos.get(0).get("text"), list.get(0).getCellByName("text").getCellValue());

        assertEquals(mapCantos.get(1).get("canto"), list.get(1).getCellByName("canto").getCellValue());
        assertEquals(mapCantos.get(1).get("text"), list.get(1).getCellByName("text").getCellValue());


    }


}
