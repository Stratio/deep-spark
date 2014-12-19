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
package com.stratio.deep.aerospike.reader;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.stratio.deep.aerospike.config.AerospikeDeepJobConfig;
import com.stratio.deep.aerospike.partition.AerospikePartition;
import org.apache.spark.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Helper class for reading data from Aerospike.
 */
public class AerospikeReader {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeReader.class);

    private AerospikeClient aerospikeClient;

    private AerospikeDeepJobConfig deepJobConfig;

    private RecordSet recordSet;

    /**
     * Public constructor.
     * @param deepJobConfig Aerospike configuration object.
     */
    public AerospikeReader(AerospikeDeepJobConfig deepJobConfig) {
        this.deepJobConfig = deepJobConfig;
    }

    /**
     * Closes the Aerospike connection and the obtained RecordSet.
     */
    public void close() {
        if(recordSet!=null) {
            recordSet.close();
        }
        if(aerospikeClient!=null) {
            aerospikeClient.close();
        }
    }

    /**
     * Initialization method.
     * @param partition
     */
    public void init(Partition partition) {
        List<Host> hosts = new ArrayList<>();
        AerospikePartition aerospikePartition = (AerospikePartition)partition;
        Host aerospikeHost = new Host(aerospikePartition.getNodeHost(), aerospikePartition.getNodePort());
        hosts.add(aerospikeHost);
        ClientPolicy clientPolicy = new ClientPolicy();
        if(deepJobConfig.getUsername() != null && deepJobConfig.getPassword() != null) {
            clientPolicy.user = deepJobConfig.getUsername();
            clientPolicy.password = deepJobConfig.getPassword();
        }

        aerospikeClient = new AerospikeClient(clientPolicy, hosts.toArray(new Host[hosts.size()]));

        recordSet = aerospikeClient.query(new QueryPolicy(), buildStatement());
    }

    /**
     * Check if RecordSet has pending elements.
     * @return True if there is another record in the RecordSet.
     */
    public boolean hasNext() {
        return recordSet.next();
    }

    /**
     * Returns the next Aerospike Record from the RecordSet.
     * @return Next Aerospike Record from the RecordSet.
     */
    public Record next() {
        return recordSet.getRecord();
    }

    private Statement buildStatement() {
        Statement statement = new Statement();
        statement.setNamespace(deepJobConfig.getNamespace());
        statement.setSetName(deepJobConfig.getSet());
        statement.setBinNames(deepJobConfig.getInputColumns());
        statement.setFilters(buildAerospikeFilters());
        return statement;
    }

    private Filter[] buildAerospikeFilters() {
        if(deepJobConfig.getEqualsFilter() != null) {
            Tuple2<String, Object> equalsFilter = deepJobConfig.getEqualsFilter();
            Filter filter = Filter.equal(equalsFilter._1(),
                    getAerospikeValueFromObject(equalsFilter._2()));
            return new Filter[] {filter};
        } else if(deepJobConfig.getNumrangeFilter() != null) {
            Tuple3<String, Object, Object> numRangeFilter = deepJobConfig.getNumrangeFilter();
            Filter filter = Filter.range(numRangeFilter._1(),
                    getAerospikeValueFromObject(numRangeFilter._2()),
                    getAerospikeValueFromObject(numRangeFilter._3()));
            return new Filter[] {filter};
        }
        return new Filter[] {};
    }

    private Value getAerospikeValueFromObject(Object object) {
        Value value;
        if(object.getClass().equals(Integer.class)) {
            value = new Value.IntegerValue((Integer)object);
        } else if(object.getClass().equals(Long.class)) {
            value = new Value.LongValue((Long)object);
        } else if(object.getClass().equals(String.class)) {
            value = new Value.StringValue((String)object);
        } else if(object instanceof byte[]) {
            value = new Value.BytesValue((byte[])object);
        } else if(List.class.isAssignableFrom(object.getClass())) {
            value = new Value.ListValue((List)object);
        } else if(Map.class.isAssignableFrom(object.getClass())) {
            value = new Value.MapValue((Map)object);
        } else if(object == null) {
            value = new Value.NullValue();
        } else {
            throw new IllegalArgumentException("Aerospike filters do not support [" + object.getClass() + "] values");
        }
        return value;
    }

}
