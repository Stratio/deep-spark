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

import com.stratio.deep.testentity.CommonsTestEntity;
import com.stratio.deep.testentity.MongoDBTestEntity;
import com.stratio.deep.utils.UtilMongoDB;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;

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


}
