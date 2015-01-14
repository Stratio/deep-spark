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
package com.stratio.deep.core.rdd;

import static com.stratio.deep.commons.utils.Utils.getExtractorInstance;
import static com.stratio.deep.core.util.ExtractorClientUtil.getExtractorClient;
import static scala.collection.JavaConversions.asScalaBuffer;
import static scala.collection.JavaConversions.asScalaIterator;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.exception.DeepExtractorInitializationException;
import com.stratio.deep.commons.exception.DeepIOException;
import com.stratio.deep.commons.rdd.IExtractor;

import scala.Function1;
import scala.Unit;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

/**
 * Created by rcrespo on 11/08/14.
 */
public class DeepRDD<T, S extends BaseConfig> extends RDD<T> implements Serializable {

    private static final long serialVersionUID = -5360986039609466526L;

    private transient IExtractor<T, S> extractorClient;

    protected Broadcast<S> config;

    public Broadcast<S> getConfig() {
        return config;
    }

    public DeepRDD(SparkContext sc, S config) {
        super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(config
                .getEntityClass()));
        config.setRddId(id());
        this.config =
                sc.broadcast(config, ClassTag$.MODULE$
                        .<S>apply(config.getClass()));

    }

    @Override
    public Seq<String> getPreferredLocations(Partition split) {
        initExtractorClient();

        List<String> locations = extractorClient.getPreferredLocations(split);
        if (locations == null || locations.isEmpty()) {
            return super.getPreferredLocations(split);
        }

        return asScalaBuffer(locations);

    }

    @Override
    public Iterator<T> compute(Partition split, TaskContext context) {

        initExtractorClient();

        extractorClient.initIterator(split, config.getValue());

        context.addTaskCompletionListener(new AbstractFunction1<TaskContext, Unit> (){

            @Override
            public Unit apply(TaskContext v1) {
                extractorClient.close();
                return null;
            }
        });

        java.util.Iterator<T> iterator = new java.util.Iterator<T>() {

            @Override
            public boolean hasNext() {
                return extractorClient.hasNext();
            }

            @Override
            public T next() {
                return extractorClient.next();
            }

            @Override
            public void remove() {
                throw new DeepIOException(
                        "Method not implemented (and won't be implemented anytime soon!!!)");
            }
        };

        return new InterruptibleIterator<>(context, asScalaIterator(iterator));

    }

    @Override
    public Partition[] getPartitions() {
        initExtractorClient();
        return extractorClient.getPartitions(config.getValue());
    }

    /**
     * It tries to get an Extractor Instance,
     * if there is any problem try to instance an extractorClient
     */
    private void initExtractorClient() {
        try {
            if (extractorClient == null) {
                extractorClient = getExtractorInstance(config.getValue());
            }
        } catch (DeepExtractorInitializationException e) {
            extractorClient = getExtractorClient();
        }

    }

}
