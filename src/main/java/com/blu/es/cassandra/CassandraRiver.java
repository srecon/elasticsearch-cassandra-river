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
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.ThreadExecutor;
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
    private static final String SETTINGS_KEY_CASSANDRA="cassandra";
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRiver.class);


    private String hostName;
    private String dcName;
    private String port;
    private String clusterName;
    private String keySpace;
    private static String columnFamily;
    private static int batchSize;
    private static Client client;

    private static String typeName;
    private static String indexName;
    private String cron;

    private static CassandraFactory cassandraFactory;


    @Inject
    @SuppressWarnings("unchecked")
    protected CassandraRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;
        // read settings
        if(settings != null && settings.settings().containsKey(SETTINGS_KEY_CASSANDRA)){
            Map<String, Object> couchSettings = (Map<String, Object>) settings.settings().get("cassandra");
            this.clusterName = XContentMapValues.nodeStringValue(couchSettings.get("cluster_name"), "CRM-MNP Cluster");
            this.keySpace = XContentMapValues.nodeStringValue(couchSettings.get("keyspace"), "mnpkeyspace");
            this.columnFamily = XContentMapValues.nodeStringValue(couchSettings.get("column_family"), "event_log");
            this.batchSize = XContentMapValues.nodeIntegerValue(couchSettings.get("batch_size"), 1000);
            this.hostName = XContentMapValues.nodeStringValue(couchSettings.get("hosts"), "192.168.202.115,192.168.202.116");
            this.dcName =  XContentMapValues.nodeStringValue(couchSettings.get("dcName"), "MNPANDC");
            this.cron = XContentMapValues.nodeStringValue(couchSettings.get("cron"), "0/30 * * * * ?"); // DEFAULT every 30 second
        }
        if ( settings!=null &&  settings.settings().containsKey("index")) {
            Map<String, Object> couchSettings = (Map<String, Object>) settings.settings().get("index");
            this.indexName = XContentMapValues.nodeStringValue(couchSettings.get("index"), "DEFAULT_INDEX_NAME");
            this.typeName = XContentMapValues.nodeStringValue(couchSettings.get("type"), "DEFAULT_TYPE_NAME");

        } else {
            this.indexName = "DEFAULT_INDEX_NAME";
            this.typeName = "DEFAULT_TYPE_NAME";
        }
        // init factory
        cassandraFactory = CassandraFactory.getInstance(getKeySpace(), getHostName(), getPort(), getDcName());
    }

    @Override
    public void start() {
        LOGGER.info("Cassandra River Start!!" + "batch size {}", getBatchSize());

        JobDetail jobDetail = JobBuilder.newJob(RiverJOb.class).withIdentity("River Job","river").build();
        //trigger
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("RiverTrigger", "river")
                .startNow()
                .withSchedule(CronScheduleBuilder.cronSchedule(getCron()))
                .build();
        // schedule
        Scheduler scheduler;
        try {
            scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.start();
            scheduler.scheduleJob(jobDetail, trigger);

        } catch (SchedulerException e) {
            LOGGER.info("Scheduler Exception {}", e);
        }

    }

    @Override
    public void close() {
        LOGGER.info("Cassandra River Close!!");
        client.admin().indices().prepareDeleteMapping("_river").setType("cassandra-river").execute();
        cassandraFactory.shutdown();
    }

    // Quartz Job
    @DisallowConcurrentExecution
    public static class RiverJOb implements Job{
        public RiverJOb() {     }

        @Override
        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
            LOGGER.info("Quartz Job");
            // executor to index
            ThreadFactory daemonThreadFactory = new ThreadFactoryBuilder().setNameFormat("Queue-Indexer-thread-%d").setDaemon(false).build();
            ExecutorService threadExecutor = Executors.newFixedThreadPool(20, daemonThreadFactory);

            Session session =  cassandraFactory.getSession();

            String SQL = "select * from " + getColumnFamily() +" ALLOW FILTERING;";
            PreparedStatement statement = session.prepare(SQL);

            BoundStatement bndStm = new BoundStatement(statement);
            bndStm.setFetchSize(batchSize);

            ResultSet result = session.execute(bndStm.bind());
            Iterator ite = result.iterator();

            //CassandraCFData cfData = new CassandraCFData();
            Map<String, Map<String, String>>  values = new HashMap<String, Map<String, String>>();
            while(ite.hasNext()){
                Row row = (Row) ite.next();
                //values = new HashMap<String, Map<String, String>>();
                ColumnDefinitions columnDefinitions =  row.getColumnDefinitions();
                String rowId = UUID.randomUUID().toString();
                Map<String, String> rows = new HashMap<String, String>();

                //logger.info("Column defination:{}", columnDefinitions);
                for(int i = 0; i < columnDefinitions.size(); i++){
                    String columnName = columnDefinitions.getName(i);
                    String columnValue="";
                    DataType dataType = columnDefinitions.getType(i);

                    columnValue = CassandraFactory.getStringValue(dataType, row, columnName);

                    rows.put(columnName, columnValue);
                }
                values.put(rowId, rows);
                if(values.size() >= getBatchSize()){
                    // copy hash map
                    Map<String, Map<String, String>>  tmpValues = new HashMap<String, Map<String, String>>();
                    tmpValues.putAll(values);
                    threadExecutor.execute(new Indexer(getBatchSize(),tmpValues, getTypeName(), getIndexName()));
                    values = new HashMap<String, Map<String, String>>();
                }
            }
            if(values.size() < getBatchSize()){
                threadExecutor.execute(new Indexer(getBatchSize(),values, getTypeName(), getIndexName()));
            }

        }
    }

    private static class Indexer implements Runnable{
        private final int batchSize;
        private final Map<String, Map<String, String>> keys;
        private final String typeName;
        private final String indexName;

        public Indexer(int batchSize, Map<String, Map<String, String>> keys, String typeName, String indexName) {
            this.batchSize = batchSize;
            this.keys = keys;
            this.typeName = typeName;
            this.indexName = indexName;
        }

        @Override
        public void run() {
            LOGGER.info("Starting thread with Data {}, batch size {}", this.keys.size(), this.batchSize);
            BulkRequestBuilder bulk = client.prepareBulk();
            for(String key : this.keys.keySet()){
                try{
                    bulk.add(Requests.indexRequest(this.indexName).type(this.typeName)
                            .id(key).source(this.keys.get(key)));

                } catch(Exception e){
                    LOGGER.error("Exception in run {}", e);
                }
            }
            saveToEs(bulk);
        }
        private boolean saveToEs(BulkRequestBuilder bulkRequestBuilder){
            LOGGER.info("Inserting {} keys in ES", bulkRequestBuilder.numberOfActions());
            try{
                bulkRequestBuilder.execute().addListener(new Runnable() {
                    @Override
                    public void run() {
                        LOGGER.info("Processing Done!!");
                    }
                });
            } catch(Exception e){
                LOGGER.info("Exception in persistance {}", e);
            }
                return false;
            }

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

    public static String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public static int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public static String getTypeName() {
        return typeName;
    }

    public static String getIndexName() {
        return indexName;
    }

    public String getCron() {
        return cron;
    }
}
