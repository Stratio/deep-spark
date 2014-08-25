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


import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.core.extractor.client.ExtractorClient;
import com.stratio.deep.exception.DeepExtractorinitializationException;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.rdd.IExtractor;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;

import java.io.Serializable;

import static scala.collection.JavaConversions.asScalaIterator;


/**
 * Created by rcrespo on 11/08/14.
 */
public class DeepRDD<T> extends RDD<T> implements Serializable {

    private static final long serialVersionUID = -5360986039609466526L;

    private transient IExtractor<T> extractorClient;

    protected Broadcast<ExtractorConfig<T>> config;

    public Broadcast<ExtractorConfig<T>> getConfig() {
        return config;
    }

    public DeepRDD(SparkContext sc, ExtractorConfig<T> config) {
        super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(config
                .getEntityClass()));
        config.putValue("spark.rdd.id", String.valueOf(id()));
        this.config =
                sc.broadcast(config, ClassTag$.MODULE$
                        .<ExtractorConfig<T>>apply(config.getClass()));


        initExtractorClient();

    }

    public DeepRDD(SparkContext sc, Class entityClass) {
        super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(entityClass));
    }

    @Override
    public Iterator<T> compute(Partition split, TaskContext context) {

        initExtractorClient();

        context.addOnCompleteCallback(new OnComputedRDDCallback(extractorClient));


        extractorClient.initIterator(split, config.getValue());
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



    private void initExtractorClient() {
        try {
            if (extractorClient == null) {
                extractorClient = new ExtractorClient<>();
                ((ExtractorClient) extractorClient).initialize();
            }
        } catch (DeepExtractorinitializationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


}
