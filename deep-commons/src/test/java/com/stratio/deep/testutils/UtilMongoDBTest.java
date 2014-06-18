package com.stratio.deep.testutils;

import com.stratio.deep.testentity.CommonsBaseTestEntity;
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
