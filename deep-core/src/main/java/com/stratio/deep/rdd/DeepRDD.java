package com.stratio.deep.rdd;


import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.extractor.client.ExtractorClient;
import com.stratio.deep.partition.impl.DeepPartition;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;

import javax.net.ssl.SSLException;
import java.io.Serializable;

import static scala.collection.JavaConversions.asScalaIterator;


/**
 * Created by rcrespo on 11/08/14.
 */
public class DeepRDD<T> extends RDD<T> implements Serializable {

    private static final long serialVersionUID = -5360986039609466526L;


    private transient ExtractorClient<T> extractorClient;


    protected Broadcast<ExtractorConfig<T>> config;

    public DeepRDD(SparkContext sc, ExtractorConfig<T> config) {
        super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(config
                .getEntityClass()));
        this.config =
                sc.broadcast(config, ClassTag$.MODULE$
                        .<ExtractorConfig<T>>apply(config.getClass()));

        try {
            ExtractorClient<T> extractor = new ExtractorClient<>();
            extractor.initialize();
        } catch (SSLException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public DeepRDD(SparkContext sc, Class entityClass) {
        super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(entityClass));
    }

    @Override
    public Iterator<T> compute(Partition split, TaskContext context) {

        initExtractorClient();

        context.addOnCompleteCallback(new OnComputedRDDCallback(extractorClient));

        extractorClient.initIterator(((IDeepPartition) split).splitWrapper(), config.getValue());
        java.util.Iterator<T> iterator = createScalaIterator();


        return new InterruptibleIterator<>(context, asScalaIterator(iterator));

    }

    private java.util.Iterator<T> createScalaIterator() {
        return new DeepIterator<T>() {

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
    }

    @Override
    public Partition[] getPartitions() {
        initExtractorClient();

        DeepTokenRange[] tokenRanges = extractorClient.getPartitions(config.getValue());

        Partition[] partitions = new DeepPartition[tokenRanges.length];

        int i = 0;

        for (DeepTokenRange split : tokenRanges) {
            partitions[i] = new DeepPartition(id(), i, split);
            ++i;
        }

        return partitions;
    }

    public Broadcast<ExtractorConfig<T>> getConfig() {
        return config;
    }


    private void initExtractorClient() {
        if (extractorClient == null) {
            extractorClient = new ExtractorClient<>();
            try {
                extractorClient.initialize();
            } catch (SSLException | InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }


}
