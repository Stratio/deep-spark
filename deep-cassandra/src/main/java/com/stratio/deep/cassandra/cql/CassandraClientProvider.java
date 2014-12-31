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

import static com.stratio.deep.commons.utils.Utils.quote;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.stratio.deep.cassandra.config.CassandraDeepJobConfig;
import com.stratio.deep.commons.exception.DeepIOException;
import com.stratio.deep.commons.utils.Pair;

/**
 * Created by luca on 09/04/14.
 */
class CassandraClientProvider {
    private static final transient Map<String, Session> CLIENTS_CACHE = Collections.synchronizedMap(new
            HashMap<String, Session>());

    private static final Logger LOG = LoggerFactory.getLogger(CassandraClientProvider.class);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                synchronized (CLIENTS_CACHE) {
                    if (CLIENTS_CACHE != null && !CLIENTS_CACHE.isEmpty()) {
                        LOG.info("Closing clients ");
                        for (Session entry : CLIENTS_CACHE.values()) {
                            if (entry != null) {
                                entry.close();
                            }
                        }
                    }
                }
            }
        });
    }

    CassandraClientProvider() {
    }

    static Pair<Session, String> trySessionForLocation(String location, CassandraDeepJobConfig conf,
                                                       Boolean balanced) {
        try {
            return getSession(location, conf, balanced);
        } catch (Exception e) {

            LOG.warn("Could not connect to {}, possible loss of data-locality. Delegating connection to java driver",
                    location, conf.getHost());
            return getSession(conf.getHost(), conf, true);

        }
    }

    static Pair<Session, String> getSession(String location, CassandraDeepJobConfig conf, Boolean balanced) {
        assert balanced != null;

        synchronized (CLIENTS_CACHE) {
            final int port = conf.getCqlPort();
            final String key = location + ":" + port ;

            if (CLIENTS_CACHE.containsKey(key)) {
                LOG.trace("Found cached session at level 1 for key {{}}", key);
                return Pair.create(CLIENTS_CACHE.get(key), location);
            }

            if (balanced && location.equals(conf.getHost())) {
                CLIENTS_CACHE.put(key, conf.getSession());
                LOG.trace("Found cached session at level 2 for key {{}}", key);
                return Pair.create(CLIENTS_CACHE.get(key), location);
            }

            try {

                LOG.debug("No cached session found for key {{}}", key);
                InetAddress locationInet = InetAddress.getByName(location);
                LoadBalancingPolicy loadBalancingPolicy = balanced ? Policies.defaultLoadBalancingPolicy() : new
                        LocalMachineLoadBalancingPolicy(locationInet);

                Cluster cluster = Cluster.builder()
                        .withPort(port)
                        .addContactPoints(locationInet)
                        .withLoadBalancingPolicy(loadBalancingPolicy)
                        .withProtocolVersion(ProtocolVersion.V2)
                        .withCredentials(conf.getUsername(), conf.getPassword())
                        .build();

                Session session = cluster.connect(quote(conf.getKeyspace()));
                CLIENTS_CACHE.put(key, session);

                return Pair.create(CLIENTS_CACHE.get(key), location);
            } catch (Exception e) {
                throw new DeepIOException("Failed to create authenticated client to {" + location + "}:{" + port +
                        "}", e);
            }
        }

    }
}
