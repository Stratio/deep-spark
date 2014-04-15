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

package com.stratio.deep.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.exception.DeepIllegalAccessException;
import com.stratio.deep.partition.impl.DeepPartitionLocationComparator;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.util.*;

/**
 * Created by luca on 09/04/14.
 */
class CassandraClientProvider {
    private static transient Map<String, Session> clientsCache = Collections.synchronizedMap(new HashMap<String, Session>());
    private static final Logger LOG = LoggerFactory.getLogger(CassandraClientProvider.class);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                synchronized (clientsCache) {
                    if (clientsCache != null && !clientsCache.isEmpty()) {
                        LOG.info("Closing clients ");
                        for (Session entry : clientsCache.values()) {
                            if (entry != null) {
                                entry.close();
                            }
                        }
                    }
                }
            }
        });
    }

    static Pair<Session, String> getSessionLocation(String location, IDeepJobConfig conf, Boolean balanced) {
        try {
            return getSession(location, conf, balanced);
        } catch (Exception e){
            return getSession(conf.getHost(), conf, balanced);

        }
    }

    static Pair<Session, String> getSession(String location, IDeepJobConfig conf, Boolean balanced) {
        assert balanced != null;

        synchronized (clientsCache) {
            final int port = conf.getCqlPort();
            final String key = location + ":" + port + ":" + conf.getKeyspace() + ": " + balanced;



            if (clientsCache.containsKey(key)) {
                return Pair.create(clientsCache.get(key), location);
            }

            if (location.equals(conf.getHost())) {
                clientsCache.put(key, conf.getSession());
                return Pair.create(clientsCache.get(key), location);
            }

            try {
                InetAddress locationInet = InetAddress.getByName(location);
                LoadBalancingPolicy loadBalancingPolicy = balanced ? Policies.defaultLoadBalancingPolicy() : new LocalMachineLoadBalancingPolicy(locationInet);

                Cluster cluster = Cluster.builder()
                        .withPort(port)
                        .addContactPoint(location)
                        .withLoadBalancingPolicy(loadBalancingPolicy)
                        .withCredentials(conf.getUsername(), conf.getPassword())
                        .build();

                Session session = cluster.connect(conf.getKeyspace());
                clientsCache.put(key, session);

                return Pair.create(clientsCache.get(key), location);
            } catch (Exception e) {

                LOG.warn("Failed to create authenticated client to {}:{}", location, port);
                throw new DeepIllegalAccessException("Failed to create authenticated client to {" + location + "}:{" + port + "}");
            }
        }

    }
}
