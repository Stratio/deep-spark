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

package com.stratio.deep.extractor;

import com.stratio.deep.config.EntityDeepJobConfigES;
import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.config.IESDeepJobConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.exception.DeepTransformException;
import com.stratio.deep.extractor.impl.GenericHadoopExtractor;
import com.stratio.deep.rdd.IExtractor;
import com.stratio.deep.utils.UtilES;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * EntityRDD to interact with mongoDB
 *
 * @param <T>
 */
public final class ESEntityExtractor<T> extends GenericHadoopExtractor<T,  Object, LinkedMapWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(ESEntityExtractor.class);
    private static final long serialVersionUID = -3208994171892747470L;



    public ESEntityExtractor(T t){
        super();
        this.deepJobConfig = new EntityDeepJobConfigES(t.getClass());
        this.inputFormat = new EsInputFormat<>() ;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T transformElement(Tuple2<Object, LinkedMapWritable> tuple, IDeepJobConfig<T, ? extends IDeepJobConfig>config ) {


        try {
            return UtilES.getObjectFromJson(config.getEntityClass(), tuple._2());
        } catch (Exception e) {
            LOG.error("Cannot convert JSON: ", e);
            throw new DeepTransformException("Could not transform from Json to Entity " + e.getMessage());
        }

    }



    /**
     * Save a RDD to ES
     *
     * @param rdd
     * @param config
     * @param <T>
     */
    public static <T extends IDeepType> void saveRDD(RDD<T> rdd, IDeepJobConfig<T, IESDeepJobConfig<T>> config) {

        JavaPairRDD<Object, JSONObject> save = rdd.toJavaRDD().mapToPair(new PairFunction<T, Object, JSONObject>() {


            @Override
            public Tuple2<Object, JSONObject> call(T t) throws Exception {
                return new Tuple2<>(null, UtilES.getJsonFromObject(t));
            }
        });


        save.saveAsNewAPIHadoopFile("file:///entity", Object.class, Object.class, EsOutputFormat.class, ((IESDeepJobConfig)config).getHadoopConfiguration());
    }


}
