package com.stratio.deep.core.rdd;


import com.stratio.deep.config.ExtractorConfig;
import com.stratio.deep.exception.DeepExtractorinitializationException;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.core.extractor.client.ExtractorClient;
import com.stratio.deep.partition.impl.DeepPartition;
import com.stratio.deep.rdd.DeepIterator;
import com.stratio.deep.rdd.DeepTokenRange;
import com.stratio.deep.rdd.IDeepPartition;
import com.stratio.deep.rdd.IExtractorClient;
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

    private transient IExtractorClient<T> extractorClient;

    protected Broadcast<ExtractorConfig<T>> config;

    public Broadcast<ExtractorConfig<T>> getConfig() {
        return config;
    }

    public DeepRDD(SparkContext sc, ExtractorConfig<T> config) {
        super(sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.<T>apply(config
                .getEntityClass()));
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
        initRemoteIterator((IDeepPartition) split);
        java.util.Iterator<T> iterator = createScalaIterator();

        return new InterruptibleIterator<>(context, asScalaIterator(iterator));

    }

    @Override
    public Partition[] getPartitions() {
        initExtractorClient();

        DeepTokenRange[] tokenRanges = recoverRemotePartitions();

        Partition[] partitions = createSparkPartitions(tokenRanges);

        return partitions;
    }




    private Partition[] createSparkPartitions(DeepTokenRange[] tokenRanges) {
        Partition[] partitions = new DeepPartition[tokenRanges.length];

        int i = 0;

        for (DeepTokenRange split : tokenRanges) {

            partitions[i] = new DeepPartition(id(), i, split);
            ++i;
        }
        return partitions;
    }

    private DeepTokenRange[] recoverRemotePartitions() {
        return extractorClient.getPartitions(config.getValue());
    }


    private void initExtractorClient() {
        try {
            if (extractorClient == null) {
                extractorClient = new ExtractorClient<>();
                extractorClient.initialize();
            }
        } catch (DeepExtractorinitializationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private void initRemoteIterator(IDeepPartition split) {
        extractorClient.initIterator(((IDeepPartition) split).splitWrapper(), config.getValue());
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


}
