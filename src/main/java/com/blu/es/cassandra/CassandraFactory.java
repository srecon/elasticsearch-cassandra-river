package com.blu.es.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * User: bsha
 * Date: 09.07.2014
 * Time: 14:34
 */
public class CassandraFactory {
    private static Cluster cluster;
    private static Session session;

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraFactory.class);
    
    private static final String DATA_TYPE_TIMESTAMP="timestamp";
    private static final String DATA_TYPE_BOOLEAN="boolean";


    private CassandraFactory() {    }
    private CassandraFactory(String keySpaceName, String hostName, String port, String dcName, String username, String password){
        LoadBalancingPolicy loadBalancingPolicy = new DCAwareRoundRobinPolicy(dcName,2);
        PoolingOptions poolingOptions = new PoolingOptions();

        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL,10);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 50);

        //cluster = Cluster.builder().addContactPoints(hostName).withCompression(ProtocolOptions.Compression.SNAPPY)
        Cluster.Builder builder = Cluster.builder().addContactPoints(hostName)
                .withPoolingOptions(poolingOptions)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                .withLoadBalancingPolicy(loadBalancingPolicy);
        if(!username.isEmpty() && !password.isEmpty()) {
            builder.withCredentials(username, password);
        }
        cluster = builder.build();
        Metadata metadata = cluster.getMetadata();
        LOGGER.info("Connected to cluster: {}", metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            LOGGER.info("Datacenter: {}; Host: {}; Rack: {}", new String[]{host.getDatacenter(), host.getAddress().getHostAddress(), host.getRack()});
        }
        session = cluster.connect(keySpaceName);
        LOGGER.info("Connection established!");
    }

    public static CassandraFactory getInstance(String keySpaceName, String hostName, String port, String dcName, String username, String password){
        return new CassandraFactory(keySpaceName, hostName, port, dcName, username, password);
    }

    public Session getSession() {
        return session;
    }
    public void shutdown(){
        if(cluster != null && !cluster.isClosed()){
            cluster.close();
            LOGGER.info("Cluster shutting down!!");
        }
    }
    /**
     * Convert Cassandra data type to String
     * */
    public static String getStringValue(DataType dataType, Row row, String columnName){
        String value= "";
        if (dataType == null){
            return value;
        }

        if(dataType.getName().toString().equalsIgnoreCase(DATA_TYPE_TIMESTAMP)){
            Date date = row.getDate(columnName);
            value = date!=null ? date.toString() :"";
        } else if(dataType.getName().toString().equalsIgnoreCase(DATA_TYPE_BOOLEAN)){
            Boolean boolValue =  row.getBool(columnName);
            value = boolValue.toString();
        } else{
            value = row.getString(columnName);
        }
        return value;
    }

    private String[] getHostArray(String propertiesVal){
        Iterable<String> values = Splitter.on(",").trimResults().split(propertiesVal);

        return Iterables.toArray(values, String.class);
    }
}
