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
package com.stratio.deep.aerospike.extractor;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.stratio.deep.aerospike.config.AerospikeDeepJobConfig;
import com.stratio.deep.aerospike.partition.AerospikePartition;
import com.stratio.deep.aerospike.reader.AerospikeReader;
import com.stratio.deep.aerospike.writer.AerospikeWriter;
import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.querybuilder.UpdateQueryBuilder;
import com.stratio.deep.commons.rdd.IExtractor;
import org.apache.spark.Partition;

import java.util.ArrayList;
import java.util.List;

/**
 * Aerospike Deep Extractor implementation using an Aerospike Client (not Hadoop Connector).
 */
public abstract class AerospikeNativeExtractor<T, S extends BaseConfig<T>> implements IExtractor<T, S> {

    protected AerospikeDeepJobConfig<T> aerospikeDeepJobConfig;

    private AerospikeReader reader;

    private AerospikeWriter writer;

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition[] getPartitions(S config) {
        AerospikeClient aerospikeClient = null;
        Cluster aerospikeCluster;

        try {

            initAerospikeDeepJobConfig(config);
            List<Host> hosts = new ArrayList<>();
            for(String hostName:aerospikeDeepJobConfig.getHostList()) {
                Host host = new Host(hostName, aerospikeDeepJobConfig.getAerospikePort());
                hosts.add(host);
            }
            ClientPolicy clientPolicy = new ClientPolicy();
            if(aerospikeDeepJobConfig.getUsername() != null && aerospikeDeepJobConfig.getPassword() != null) {
                clientPolicy.user = aerospikeDeepJobConfig.getUsername();
                clientPolicy.password = aerospikeDeepJobConfig.getPassword();
            }
            aerospikeClient = new AerospikeClient(clientPolicy, hosts.toArray(new Host[hosts.size()]));
            aerospikeCluster = new Cluster(clientPolicy, hosts.toArray(new Host[hosts.size()]));
            return calculateSplits(aerospikeClient, aerospikeCluster);
        } catch (Exception e) {
            throw new DeepGenericException(e);
        } finally {
            if (aerospikeClient != null) {
                aerospikeClient.close();
            }

        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T next() {
        return transformElement(reader.next());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if(reader != null) {
            reader.close();
        }
        if(writer != null) {
            writer.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initIterator(Partition dp, S config) {
        initAerospikeDeepJobConfig(config);
        reader = new AerospikeReader(aerospikeDeepJobConfig);
        reader.init(dp);
    }

    private void initAerospikeDeepJobConfig(S config) {
        if (config instanceof ExtractorConfig) {
            aerospikeDeepJobConfig.initialize((ExtractorConfig) config);
        } else if (aerospikeDeepJobConfig instanceof AerospikeDeepJobConfig){
            aerospikeDeepJobConfig = (AerospikeDeepJobConfig<T>) config;
        }else{
            aerospikeDeepJobConfig = aerospikeDeepJobConfig.initialize((DeepJobConfig)config);
        }
        aerospikeDeepJobConfig.initialize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveRDD(T t) {
        writer.save(transformElement(t));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getPreferredLocations(Partition split) {
        List<String> hosts = new ArrayList<>();
        AerospikePartition aerospikePartition = (AerospikePartition) split;
        hosts.add(aerospikePartition.getNodeHost() + ":" + aerospikePartition.getNodePort());
        return hosts;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initSave(S config, T first, UpdateQueryBuilder queryBuilder) {
        initAerospikeDeepJobConfig(config);
        writer = new AerospikeWriter(aerospikeDeepJobConfig);
    }

    private Partition[] calculateSplits(AerospikeClient aerospikeClient, Cluster aerospikeCluster) {
        Node[] nodes = aerospikeClient.getNodes();
        int nsplits = nodes.length;
        if (nsplits == 0) {
            throw new DeepGenericException("no Aerospike nodes found");
        }
        Partition[] partitions = new Partition[nsplits];
        for (int ii = 0; ii < nsplits; ii++) {
            Node node = nodes[ii];

            String nodeName = node.getName();

            // We want to avoid 127.0.0.1 as a hostname
            // because this value will be transferred to a
            // different hadoop node to be processed.
            //
            Host[] aliases = node.getAliases();
            Host nodehost = aliases[0];
            if (aliases.length > 1) {
                for (int jj = 0; jj < aliases.length; ++jj) {
                    if (!aliases[jj].name.equals("127.0.0.1")) {
                        nodehost = aliases[jj];
                        break;
                    }
                }
            }
            partitions[ii] = new AerospikePartition(ii, nodehost.name, nodehost.port);
        }
        return partitions;
    }

    /**
     * Transforms an Aerospike Record into another type.
     * @param record Aerospike Record to transform.
     * @return Object of type Cells or DeepEntity.
     */
    protected abstract T transformElement(Record record);

    /**
     * Transforms an object of type Cells or DeepEntity into an Aerospike Record.
     * @param entity Cells or DeepEntity object.
     * @return Aerospike Record.
     */
    protected abstract Record transformElement(T entity);
}
