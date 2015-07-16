package com.stratio.deep.cassandra.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.policies.LoadBalancingPolicy;



/**
 * Created by jmgomez on 16/07/15.
 */
public class CassandraClusterBuilder {

    public static Cluster createCassandraCluster(String host, int port, String username, String password, ProtocolVersion protocolVersion, LoadBalancingPolicy loadBalancingPolicy){
        PoolingOptions options = new PoolingOptions();
        options.setMaxConnectionsPerHost(HostDistance.LOCAL, 5);
        options.setMaxConnectionsPerHost(HostDistance.REMOTE, 500);

        Cluster.Builder builder = Cluster.builder()
                .withPort(port)
                .addContactPoints(host.split(","))
                .withCredentials(username, password)
                .withProtocolVersion(protocolVersion)
                .withPoolingOptions(options);
        if (loadBalancingPolicy!=null){
            builder.withLoadBalancingPolicy(loadBalancingPolicy);
        }

        Cluster cluster = builder.build();

        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(600000);

        return cluster;
    }


}
