package com.blu.es.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/*
    private String keySpaceName;
    private String hostName;
    private String port;
    private String dcName;
*/

    private CassandraFactory() {    }
    private CassandraFactory(String keySpaceName, String hostName, String port, String dcName){
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

    public static CassandraFactory getInstance(String keySpaceName, String hostName, String port, String dcName){
        return new CassandraFactory(keySpaceName, hostName, port, dcName);
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
    public CassandraCFData getData(final String cfName, final int batchSize){
        // read data from Cassandra by paging

        String SQL = "select * from " + cfName +" ALLOW FILTERING;";
        PreparedStatement statement = getSession().prepare(SQL);

        BoundStatement bndStm = new BoundStatement(statement);
        bndStm.setFetchSize(batchSize);

        ResultSet result = getSession().execute(bndStm.bind());
        Iterator ite = result.iterator();

        CassandraCFData cfData = new CassandraCFData();
        Map<String, Map<String, String>>  values = cfData.getData();

        while(ite.hasNext()){
            Row row = (Row) ite.next();
            ColumnDefinitions columnDefinitions =  row.getColumnDefinitions();
            String rowId = UUID.randomUUID().toString();
            Map<String, String> rows = new HashMap<String, String>();

            LOGGER.info("Column defination:{}", columnDefinitions);
            for(int i = 0; i < columnDefinitions.size(); i++){
                String columnName = columnDefinitions.getName(i);
                String columnValue="";
                DataType dataType = columnDefinitions.getType(i);

                columnValue = getStringValue(dataType, row, columnName);

                rows.put(columnName, columnValue);
            }
            values.put(rowId, rows);
        }
        return cfData;
    }
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
