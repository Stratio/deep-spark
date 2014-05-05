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

package com.stratio.deep.testutils;

import org.apache.commons.lang.StringUtils;

/**
 * Common properties used by the examples.
 * <p/>
 * Author: Emmanuelle Raffenne
 * Date..: 26-feb-2014
 */
public class ContextProperties {

    /**
     * spark cluster endpoint.
     */
    private String cluster;

    /**
     * spark home
     */
    private String sparkHome;

    /**
     * The jar to be added to the spark context.
     */
    private String jar;

    /**
     * Endpoint of the cassandra cluster against which the examples will be run. Defaults to 'localhost'.
     */
    private String cassandraHost;

    /**
     * Cassandra's cql port. Defaults to 9042.
     */
    private int cassandraCqlPort;

    /**
     * Cassandra's cql port. Defaults to 9160.
     */
    private int cassandraThriftPort;

    /**
     * Default constructor
     */
    public ContextProperties() {

        String sparkHomeProp = System.getProperty("sparkHome");
        String sparkMaster = System.getProperty("sparkMaster");
        String deepVersionProp = StringUtils.isNotEmpty(System.getProperty("deepVersion")) ? System.getProperty
                ("deepVersion") : "0.1.3-SNAPSHOT";
        String cassandraHostProp = System.getProperty("cassandraHost");
        String cassandraCqlPortProp = System.getProperty("cassandraCqlPort");
        String cassandraThriftPortProp = System.getProperty("cassandraThriftPort");

        cluster = StringUtils.isNotEmpty(sparkMaster) ? sparkMaster : "local";
        sparkHome = StringUtils.isNotEmpty(sparkHomeProp) ? sparkHomeProp : "/opt/SDS/deep";
        jar = "/tmp/StratioDeepExamples-" + deepVersionProp + ".jar";
        cassandraHost = StringUtils.isNotEmpty(cassandraHostProp) ? cassandraHostProp : "localhost";
        cassandraCqlPort = StringUtils.isNotEmpty(cassandraCqlPortProp) ? Integer.parseInt(cassandraCqlPortProp) : 9042;
        cassandraThriftPort = StringUtils.isNotEmpty(cassandraThriftPortProp) ? Integer.parseInt
                (cassandraThriftPortProp) : 9160;
    }

    /**
     * Public constructor.
     */
    public ContextProperties(String[] args) {
        this();

        if (args != null && args.length > 0) {
            cluster = args[0];
            sparkHome = args[1];
            jar = args[2];
            cassandraHost = args[3];
            cassandraCqlPort = Integer.parseInt(args[4]);
            cassandraThriftPort = Integer.parseInt(args[5]);
        }


    }

    public String getCluster() {
        return cluster;
    }

    public String getSparkHome() {
        return sparkHome;
    }

    public String getJar() {
        return jar;
    }

    public String getCassandraHost() {
        return cassandraHost;
    }

    public int getCassandraCqlPort() {
        return cassandraCqlPort;
    }

    public int getCassandraThriftPort() {
        return cassandraThriftPort;
    }
}
