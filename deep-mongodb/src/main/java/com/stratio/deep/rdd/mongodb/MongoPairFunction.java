package com.stratio.deep.rdd.mongodb;

import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;
import scala.Tuple2;

/**
 * Created by rcrespo on 11/06/14.
 */
public class MongoPairFunction implements PairFunction<BSONObject, Object, BSONObject> {


    @Override
    public Tuple2<Object, BSONObject> call(BSONObject bsonObject) throws Exception {
        return new Tuple2<>(null, bsonObject);
    }
}
