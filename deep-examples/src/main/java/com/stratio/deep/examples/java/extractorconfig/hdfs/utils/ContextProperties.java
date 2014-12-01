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

package com.stratio.deep.examples.java.extractorconfig.hdfs.utils;

import static org.apache.commons.lang.StringUtils.defaultIfEmpty;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.utils.Constants;

/**
 * Common properties used by the examples.
 */
public class ContextProperties {
    static final String OPT_INT = "int";

    private static final Logger LOG = Logger.getLogger(ContextProperties.class);
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
    private String[] jar;

    /**
     * Endpoint of the mongo cluster against which the examples will be run. Defaults to 'localhost'.
     */
    private String hdfsCluster;

    /**
     * Public constructor.
     */
    public ContextProperties(String[] args) {
        Options options = new Options();

        options.addOption("m", "master", true, "The spark's master endpoint");
        options.addOption("h", "sparkHome", true, "The spark's home, eg. /opt/spark");
        options.addOption("H", "hdfsHost", true, "hdfs endpoint");

        options.addOption(
                OptionBuilder.hasArg().withType(Integer.class).withLongOpt("hdfsCqlPort").withArgName("hdfs_cql_port")
                        .withDescription("hdfs's cql port, defaults to 9042").create());
        options.addOption(OptionBuilder.hasArg().withType(Integer.class).withLongOpt("hdfsThriftPort")
                .withArgName("hdfs_thrift_port").withDescription("hdfs's thrift port, " +
                        "defaults to 9160").create());
        options.addOption(OptionBuilder.hasArg().withValueSeparator(',').withLongOpt("jars")
                .withArgName("jars_to_add").withDescription("comma separated list of jars to add").create());
        Option help = new Option("help", "print this message");
        //options.addOption("j","jars", true, "comma separated list of jars to add");
        options.addOption(help);
        CommandLineParser parser = new PosixParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                formatter.printHelp("", options);

                throw new DeepGenericException("Help command issued");
            }

            jar = (line.hasOption("jars") ? line.getOptionValues("jars") : new String[] { });
            cluster = line.getOptionValue("master", defaultIfEmpty(System.getProperty("spark.master"), "local"));
            sparkHome = line.getOptionValue("sparkHome", defaultIfEmpty(System.getProperty("spark.home"), ""));
            hdfsCluster = line.getOptionValue("hdfsHost", Constants.DEFAULT_HDFS_HOST);

        } catch (ParseException e) {
            formatter.printHelp("", options);
            LOG.error("Unexpected exception: ", e);
        }
    }

    public String getCluster() {
        return cluster;
    }

    public String getSparkHome() {
        return sparkHome;
    }

    public String[] getJars() {
        return jar;
    }

}
