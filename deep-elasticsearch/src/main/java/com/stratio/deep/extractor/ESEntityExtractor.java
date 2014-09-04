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
import com.stratio.deep.commons.config.IDeepJobConfig;
import com.stratio.deep.commons.exception.DeepTransformException;
import com.stratio.deep.commons.extractor.impl.GenericHadoopExtractor;
import com.stratio.deep.utils.UtilES;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;

/**
 * EntityRDD to interact with mongoDB
 *
 * @param <T>
 */
public final class ESEntityExtractor<T> extends GenericHadoopExtractor<T,  Object, LinkedMapWritable, Object, LinkedMapWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(ESEntityExtractor.class);
    private static final long serialVersionUID = -3208994171892747470L;



    public ESEntityExtractor(Class<T> t){
        super();
        this.deepJobConfig = new EntityDeepJobConfigES(t);
        this.inputFormat = new EsInputFormat<>() ;
        this.outputFormat = new EsOutputFormat() ;

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

    @Override
    public Tuple2<Object, LinkedMapWritable> transformElement(T record) {
        try {
            return new Tuple2<>(null, UtilES.getLinkedMapWritableFromObject(record));
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            LOG.error(e.getMessage());
            throw new DeepTransformException(e.getMessage());
        }
    }

}
