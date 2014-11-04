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

package com.stratio.deep.cassandra.cql;

import static com.stratio.deep.cassandra.util.CassandraUtils.updateQueryGenerator;
import static com.stratio.deep.commons.utils.Utils.quote;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.dht.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.deep.cassandra.config.ICassandraDeepJobConfig;
import com.stratio.deep.commons.entity.Cells;

/**
 * Handles the distributed write to cassandra in batch.
 */
public class DeepIncrementalCqlRecordWriter extends DeepCqlRecordWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DeepIncrementalCqlRecordWriter.class);

    /**
     * Con
     * 
     * @param writeConfig
     */
    public DeepIncrementalCqlRecordWriter(ICassandraDeepJobConfig writeConfig) {
        super(writeConfig);
    }

    /**
     * Adds the provided row to a batch. If the batch size reaches the threshold configured in
     * IDeepJobConfig.getBatchSize the batch will be sent to the data store.
     * 
     * @param keys
     *            the Cells object containing the row keys.
     * @param values
     *            the Cells object containing all the other row columns.
     */
    @Override
    public void write(Cells keys, Cells values) {
        /* generate SQL */
        String localCql = updateQueryGenerator(keys, values, writeConfig.getKeyspace(),
                quote(writeConfig.getColumnFamily()));

        Token range = partitioner.getToken(getPartitionKey(keys));

        // add primary key columns to the bind variables
        List<Object> allValues = new ArrayList<>(values.getCellValues());
        allValues.addAll(keys.getCellValues());

        // get the client for the given range, or create a new one
        RangeClient client = clients.get(range);
        if (client == null) {
            // haven't seen keys for this range: create new client
            client = new RangeClient();
            clients.put(range, client);
        }

        if (client.put(localCql, allValues)) {
            removedClients.put(range, clients.remove(range));
        }

    }
}
