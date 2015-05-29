package com.blu.es.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

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
    private Map<String, Session> sessions = new HashMap<String, Session>();

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraFactory.class);

    private CassandraFactory() {    }
    private CassandraFactory(String hostName, String port, String dcName, String username, String password, Integer connectTimeoutMillis, Integer readTimeoutMillis){
        LoadBalancingPolicy loadBalancingPolicy = new DCAwareRoundRobinPolicy(dcName,2);
        PoolingOptions poolingOptions = new PoolingOptions();

        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL,10);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 50);

        SocketOptions socketOptions = new SocketOptions().setReadTimeoutMillis(readTimeoutMillis)
          .setConnectTimeoutMillis(connectTimeoutMillis);

        Cluster.Builder builder = Cluster.builder().addContactPoints(hostName)
                .withPoolingOptions(poolingOptions)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withSocketOptions(socketOptions);
        if(!username.isEmpty() && !password.isEmpty()) {
            builder.withCredentials(username, password);
        }
        cluster = builder.build();
        Metadata metadata = cluster.getMetadata();
        LOGGER.info("Connected to cluster: {}", metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            LOGGER.info("Datacenter: {}; Host: {}; Rack: {}", new String[]{ host.getDatacenter(), host.getAddress().getHostAddress(), host.getRack() });
        }
    }

    public static CassandraFactory getInstance(String hostName, String port, String dcName, String username, String password, Integer connectTimeoutMillis, Integer readTimeoutMillis){
        return new CassandraFactory(hostName, port, dcName, username, password, connectTimeoutMillis, readTimeoutMillis);
    }

    public Session getSession(String keySpaceName) {
        Session session;
        if(this.sessions.containsKey(keySpaceName)) {
            session = this.sessions.get(keySpaceName);
        } else {
            session = cluster.connect(keySpaceName);
            LOGGER.info(String.format("Connection established to %s!", keySpaceName));
            this.sessions.put(keySpaceName, session);
        }
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

        switch(dataType.getName()) {
            case BIGINT:
            case COUNTER: {
                Long raw = row.getLong(columnName);
                value = raw != null ? raw.toString() : "";
                break;
            }
            case BLOB: {
                ByteBuffer raw = row.getBytes(columnName);
                value = raw != null ? raw.toString() : "";
                break;
            }
            case BOOLEAN: {
                Boolean raw = row.getBool(columnName);
                value = raw.toString();
                break;
            }
            case DECIMAL: {
                BigDecimal raw = row.getDecimal(columnName);
                value = raw != null ? raw.toString() : "";
                break;
            }
            case DOUBLE: {
                Double raw = row.getDouble(columnName);
                value = raw != null ? raw.toString() : "";
                break;
            }
            case FLOAT: {
                Float raw = row.getFloat(columnName);
                value = raw != null ? raw.toString() : "";
                break;
            }
            case INET: {
                InetAddress raw = row.getInet(columnName);
                value = raw != null ? raw.toString() : "";
                break;
            }
            case INT: {
                Integer raw = row.getInt(columnName);
                value = raw != null ? raw.toString() : "";
                break;
            }
            case LIST: {
                List<DataType> typeArgs = dataType.getTypeArguments();
                List<?> raw = row.getList(columnName, typeArgs.get(0).asJavaClass());
                StringBuilder builder = new StringBuilder(); // Not sure if this 100% correct when working with array mapping.
                for (Iterator<?> it = raw.iterator(); it.hasNext();)
                    builder.append(it.next()).append(it.hasNext() ? "," : "");
                value = builder.toString();
                break;
            }
            case MAP: {
                List<DataType> typeArgs = dataType.getTypeArguments();
                Map<?, ?> raw = row.getMap(columnName, typeArgs.get(0).asJavaClass(), typeArgs.get(1).asJavaClass());
                value = raw != null ? raw.toString() : "";
                break;
            }
            case SET: {
                List<DataType> typeArgs = dataType.getTypeArguments();
                Set<?> raw = row.getSet(columnName, typeArgs.get(0).asJavaClass());
                value = raw != null ? raw.toString() : "";
                break;
            }
            case TIMESTAMP: {
                Date raw = row.getDate(columnName);
                value = raw != null ? String.valueOf(raw.getTime()) : "";
                break;
            }
            case TIMEUUID:
            case UUID: {
                UUID raw = row.getUUID(columnName);
                value = raw != null ? raw.toString() : "";
                break;
            }
            case VARINT: {
                BigInteger raw = row.getVarint(columnName);
                value = raw != null ? raw.toString() : "";
                break;
            }
            default: { // Column types VARCHAR, TEXT or ASCII.
                value = row.getString(columnName);
            }
        }
        return value;
    }
}
