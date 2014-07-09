package com.blu.es.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: bsha
 * Date: 08.07.2014
 * Time: 15:56
 */
public class CassandraConnectionFactory {

    private Cluster cluster;
    private Session session;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraConnectionFactory.class);

    public CassandraConnectionFactory(String keySpaceName, String hostName, String port, String dcName) {
        LoadBalancingPolicy loadBalancingPolicy = new DCAwareRoundRobinPolicy(dcName,2);
        PoolingOptions poolingOptions = new PoolingOptions();

        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL,10);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 50);


        //cluster = Cluster.builder().addContactPoints(hostName).withCompression(ProtocolOptions.Compression.SNAPPY)
        cluster = Cluster.builder().addContactPoints(hostName)
                .withPoolingOptions(poolingOptions)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .build();
        Metadata metadata = cluster.getMetadata();
        LOGGER.info("Connected to cluster: {}", metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            LOGGER.info("Datacenter: {}; Host: {}; Rack: {}", new String[]{host.getDatacenter(), host.getAddress().getHostAddress(), host.getRack()});
        }
        session = cluster.connect(keySpaceName);
        LOGGER.info("Connection established!");
    }

    public void shutdown(){
        if(cluster != null && !cluster.isClosed()){
            //cluster.shutdown();
            cluster.close();
            LOGGER.info("Cluster shutting down!!");
        }
    }

    public Session getSession(){
        return this.session;
    }
}
