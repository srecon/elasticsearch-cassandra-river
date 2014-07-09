package com.blu.es.cassandra;

import com.blu.es.plugin.river.CassandraRiverPlugin;
import com.datastax.driver.core.*;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadFactoryBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * User: bsha
 * Date: 08.07.2014
 * Time: 14:28
 */
public class CassandraRiver extends AbstractRiverComponent implements River {
    private static final String SETTINGS_KEY_CASSAANDRA="cassandra";
    private static final String DATA_TYPE_TIMESTAMP="timestamp";
    private static final String DATA_TYPE_BOOLEAN="boolean";


    private String hostName;
    private String dcName;
    private String port;
    private String clusterName;
    private String keySpace;
    private String columnFamily;
    private int batchSize;
    private Client client;

    private String typeName;
    private String indexName;

    private ExecutorService threadExecutor;
    private volatile boolean closed;



    @Inject
    protected CassandraRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;
        // read settings
        if(settings != null && settings.settings().containsKey(SETTINGS_KEY_CASSAANDRA)){
            Map<String, Object> couchSettings = (Map<String, Object>) settings.settings().get("cassandra");
            this.clusterName = XContentMapValues.nodeStringValue(couchSettings.get("cluster_name"), "CRM-MNP Cluster");
            this.keySpace = XContentMapValues.nodeStringValue(couchSettings.get("keyspace"), "mnpkeyspace");
            this.columnFamily = XContentMapValues.nodeStringValue(couchSettings.get("column_family"), "event_log");
            this.batchSize = XContentMapValues.nodeIntegerValue(couchSettings.get("batch_size"), 1000);
            this.hostName = XContentMapValues.nodeStringValue(couchSettings.get("hosts"), "192.168.202.115,192.168.202.116");
            this.dcName =  XContentMapValues.nodeStringValue(couchSettings.get("dcName"), "MNPANDC");
        }
        //@todo add index section
        if (settings.settings().containsKey("index")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> couchSettings = (Map<String, Object>) settings.settings().get("index");
            this.indexName = XContentMapValues.nodeStringValue(couchSettings.get("index"), "DEFAULT_INDEX_NAME");
            this.typeName = XContentMapValues.nodeStringValue(couchSettings.get("type"), "DEFAULT_TYPE_NAME");

        } else {
            this.indexName = "DEFAULT_INDEX_NAME";
            this.typeName = "DEFAULT_TYPE_NAME";
        }
    }

    @Override
    public void start() {
        logger.info("Cassandra River Start!!");
        // read data from Cassandra by paging
        CassandraConnectionFactory factory = new CassandraConnectionFactory(getKeySpace(), getHostName(), getPort(), getDcName());
        Session session =  factory.getSession();
        String SQL = "select * from " + getColumnFamily() +" ALLOW FILTERING;";
        PreparedStatement statement = session.prepare(SQL);

        BoundStatement bndStm = new BoundStatement(statement);
        bndStm.setFetchSize(20000);

        ResultSet result = session.execute(bndStm.bind());
        Iterator ite = result.iterator();

        CassandraCFData cfData = new CassandraCFData();
        Map<String, Map<String, String>>  values = cfData.getData();

        while(ite.hasNext()){
            Row row = (Row) ite.next();
            ColumnDefinitions columnDefinitions =  row.getColumnDefinitions();
            String rowId = UUID.randomUUID().toString();
            Map<String, String> rows = new HashMap<String, String>();

            logger.info("Column defination:{}", columnDefinitions);
            // by column
            for(int i = 0; i < columnDefinitions.size(); i++){
                String columnName = columnDefinitions.getName(i);
                String columnValue="";
                DataType dataType =  columnDefinitions.getType(i);
                logger.info("DataType:{}", dataType.getName());

                columnValue = getStringValue(dataType, row, columnName);

                logger.info("Column Name:" + columnName +"|"+ "Column value:"+ columnValue);
                // add to map
                rows.put(columnName, columnValue);
            }
            values.put(rowId, rows);
            logger.info("Row:{}", row);
        }
        // executor to index
        ThreadFactory daemonThreadFactory = new ThreadFactoryBuilder().setNameFormat("Queue-Indexer-thread-%d").setDaemon(false).build();
        threadExecutor = Executors.newFixedThreadPool(10, daemonThreadFactory);
        //while(true){
        //    if(closed){
        //        return;
        //    }
            threadExecutor.execute(new Indexer(getBatchSize(),cfData, getTypeName(), getIndexName()));

        //}

        // @todo shutdown cluster only on close
        //factory.shutdown();
    }

    @Override
    public void close() {
        logger.info("Cassandra River Close!!");
        //client.admin().indices().prepareDeleteMapping("_river").setType("cassandra-river").execute();
    }
    public class Indexer implements Runnable{
        private final int batchSize;
        private final CassandraCFData data;
        private final String typeName;
        private final String indexName;

        public Indexer(int batchSize, CassandraCFData keys, String typeName, String indexName) {
            this.batchSize = batchSize;
            this.data = keys;
            this.typeName = typeName;
            this.indexName = indexName;
        }

        @Override
        public void run() {
            logger.info("Starting thread with Data {}", this.data.getData().size());
            // get Bulk from client
            BulkRequestBuilder bulk = client.prepareBulk();
            // fill bulk
            for(String key : this.data.getData().keySet()){
                bulk.add(Requests.indexRequest(this.indexName).type(this.typeName)
                                                              .id(key).source(this.data.getData().get(key)));
                //if(bulk.numberOfActions() >= this.batchSize){
                    saveToEs(bulk);
                    //bulk = client.prepareBulk();
                //}
            }
        }
        private void saveToEs(BulkRequestBuilder bulkRequestBuilder){
            logger.info("Inserting {} keys in ES", bulkRequestBuilder.numberOfActions());

            bulkRequestBuilder.execute().addListener(new Runnable() {
                @Override
                public void run() {
                    logger.info("Processing Done!!");
                }
            });
        }
    }
    private static String getStringValue(DataType dataType, Row row, String columnName){
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

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getDcName() {
        return dcName;
    }

    public void setDcName(String dcName) {
        this.dcName = dcName;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getKeySpace() {
        return keySpace;
    }

    public void setKeySpace(String keySpace) {
        this.keySpace = keySpace;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getIndexName() {
        return indexName;
    }
}
