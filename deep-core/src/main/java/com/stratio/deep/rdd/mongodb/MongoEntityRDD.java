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

package com.stratio.deep.rdd.mongodb;

import com.mongodb.hadoop.MongoOutputFormat;
import com.stratio.deep.config.EntityDeepJobConfig;
import com.stratio.deep.config.EntityDeepJobConfigMongoDB;
import com.stratio.deep.config.GenericDeepJobConfigMongoDB;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import com.stratio.deep.utils.Pair;
import com.stratio.deep.utils.Utils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stratio's implementation of an RDD reading and writing data from and to
 * Apache Cassandra. This implementation uses Cassandra's Hadoop API.
 * <p/>
 * We do not use Map<String,ByteBuffer> as key and value objects, since
 * ByteBuffer is not serializable.
 *
 * @author Luca Rosellini <luca@strat.io>
 */
public final class MongoEntityRDD<T extends IDeepType> extends MongoRDD<T> {

    private static final long serialVersionUID = -3208994171892747470L;



    public MongoEntityRDD(SparkContext sc, IDeepJobConfig<T> config) {
        super(sc, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected T transformElement(Pair<Object, BSONObject> elem){
        // TODO: rcrespo, implement with transformation code.

        /*
        Map<String, Cell> columnDefinitions = config.value().columnDefinitions();

        Class<T> entityClass = config.value().getEntityClass();

        EntityDeepJobConfigMongoDB<T> edjc = (EntityDeepJobConfigMongoDB) config.value();
        T instance = Utils.newTypeInstance(entityClass);

        for (Map.Entry<String, ByteBuffer> entry : elem.left.entrySet()) {
            Cell metadata = columnDefinitions.get(entry.getKey());
            AbstractType<?> marshaller = metadata.marshaller();
            edjc.setInstancePropertyFromDbName(instance, entry.getKey(), marshaller.compose(entry.getValue()));
        }

        for (Map.Entry<String, ByteBuffer> entry : elem.right.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }

            Cell metadata = columnDefinitions.get(entry.getKey());
            AbstractType<?> marshaller = metadata.marshaller();
            try {
                edjc.setInstancePropertyFromDbName(instance, entry.getKey(), marshaller.compose(entry.getValue()));
            } catch (DeepNoSuchFieldException e) {
                log().debug(e.getMessage());
            }
        }

        return instance;
        */
        return null;
    }

    public static <T> void saveEntity(JavaRDD<Tuple2<Object, BSONObject>> rdd, GenericDeepJobConfigMongoDB<T> config){


        JavaRDD<BSONObject> entityPrueba = rdd.flatMap(new FlatMapFunction<Tuple2<Object, BSONObject>, BSONObject>() {
            @Override
            public Iterable<BSONObject> call(Tuple2<Object, BSONObject> arg) {
                List<BSONObject> lista = new ArrayList<>();
                lista.add(arg._2());
                return lista;
            }
        });
        JavaPairRDD<Object, DomainEntity> save = entityPrueba.mapToPair(new PairFunction<BSONObject, Object, DomainEntity>() {

            @Override
            public Tuple2<Object, DomainEntity> call(BSONObject s) throws Exception {

                return  new Tuple2<>(null, UtilMongoDB.getObjectFromBson(DomainEntity.class, s));
            }
        });


        // Only MongoOutputFormat and config are relevant
        save.saveAsNewAPIHadoopFile("file:///bogus", Object.class, Object.class, MongoOutputFormat.class, config.configHadoop);
//        System.out.println("imprimo lo primero de lo primero "+rdd.first());

    }

}
