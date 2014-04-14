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
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.exception.DeepIllegalAccessException;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

    static Pair<Session,InetAddress> getUnbalancedSession(String host, int port, String keyspace) {

            final String key = host + ":" + port + ":" + keyspace;

            LOG.info("$$$$$$$$ Creating NEW session for key: " + key);
            Cluster cluster = Cluster.builder()
                    .withPort(port)
                    .addContactPoint(host)
                    .build();

            for (Host h : cluster.getMetadata().getAllHosts()) {
                try {
                    Cluster realCluster = Cluster.builder()
                            .withPort(port)
                            .addContactPoint(h.getAddress().getHostAddress())
                            .withLoadBalancingPolicy(new LocalMachineLoadBalancingPolicy(h.getAddress()))
                            .build();

                    Session session = realCluster.connect(keyspace);
                    return Pair.create(session, h.getAddress());

                } catch (Exception e) {
                    LOG.warn("Failed to create authenticated client to {}:{}", host, port);
                    throw new DeepIllegalAccessException("Failed to create authenticated client to {" + host + "}:{" + port + "}");
                }

            }

            throw new DeepIllegalAccessException("Cannot open a session to none of the following hosts: " + cluster.getMetadata().getAllHosts());
    }

    static Session getSession(String location, IDeepJobConfig conf) {
        synchronized (clientsCache) {
            final int port = conf.getCqlPort();
            final String key = location + ":" + port + ":" + conf.getKeyspace();

            if (clientsCache.containsKey(key)) {
                return clientsCache.get(key);
            }

            if (location.equals(conf.getHost())) {
                clientsCache.put(key, conf.getSession());
                return conf.getSession();
            }

            try {
                Cluster cluster = Cluster.builder()
                        .withPort(port)
                        .addContactPoint(location)
                        .withCredentials(conf.getUsername(), conf.getPassword())
                        .build();

                Session session = cluster.connect(conf.getKeyspace());
                clientsCache.put(key, session);

                return session;
            } catch (Exception e) {
                LOG.warn("Failed to create authenticated client to {}:{}", location, port);
                throw new DeepIllegalAccessException("Failed to create authenticated client to {" + location + "}:{" + port + "}");
            }
        }

    }
}
