package com.stratio.deep.cql;

import com.stratio.deep.util.Constants;
import org.apache.hadoop.conf.Configuration;

/**
 * Stratio Deep utility class used to get/save specific properties to
 * Hadoop' configuration object.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public final class DeepConfigHelper {

    public static final String OUTPUT_BATCH_SIZE = "output.batch.size";
    public static final String CF_METADATA = "cassandra.cf.metadata";
    public static final String ADDITIONAL_FILTER_MAP = "cassandra.additional.filters";

    public static int getOutputBatchSize(Configuration conf) {
        return conf.getInt(OUTPUT_BATCH_SIZE, Constants.DEFAULT_BATCH_SIZE);
    }

    /**
     * sets the batch size used to write to cassandra. Defaults to 100.
     *
     * @param conf
     * @param batchSize
     */
    public static void setOutputBatchSize(Configuration conf, int batchSize) {
        if (batchSize > 0) {
            conf.setInt(OUTPUT_BATCH_SIZE, batchSize);
        }
    }
}
