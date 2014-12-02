package com.blu.es.cassandra;

import com.blu.es.plugin.river.CassandraRiverPlugin;
import com.datastax.driver.core.*;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.mvel2.optimizers.impl.refl.nodes.ThisValueAccessor;
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

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
    private static final String SETTINGS_KEY_CASSANDRA = "cassandra";
    private static final String SETTINGS_KEY_CONNECTION = "connection";
    private static final String SETTINGS_KEY_KEYSPACES = "keyspaces";
    private static final String SETTINGS_KEY_SYNC = "sync";

    private static final String JOB_DATA_KEY_KEYSPACE = "KeySpace";
    private static final String JOB_DATA_KEY_BATCH_SIZE = "BatchSize";
    private static final String JOB_DATA_KEY_COLUMN_FAMILY = "ColumnFamily";
    private static final String JOB_DATA_KEY_PRIMARY_KEY = "PrimaryKey";
    private static final String JOB_DATA_KEY_INDEX_NAME = "Index";
    private static final String JOB_DATA_KEY_INDEX_TYPE = "Type";

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRiver.class);

    private List<Map<String,Object>> keyspaces = new ArrayList<Map<String,Object>>();
    private int batchSize;
    private String schedule;

    private static Client client;
    private static CassandraFactory cassandraFactory;

    @Inject
    @SuppressWarnings("unchecked")
    protected CassandraRiver(RiverName riverName, RiverSettings settings, Client nodeClient) {
        super(riverName, settings);
        client = nodeClient;
        // read settings
        if(settings != null && settings.settings().containsKey(SETTINGS_KEY_CASSANDRA)){
            Map<String, Object> cassandraSettings = (Map<String, Object>) settings.settings().get("cassandra");
            if(cassandraSettings != null && cassandraSettings.containsKey(SETTINGS_KEY_CONNECTION))
            {
                LOGGER.debug("Parsing connection data");
                Map<String, Object> connectionSettings = (Map<String, Object>) cassandraSettings.get(SETTINGS_KEY_CONNECTION);
                String hosts = XContentMapValues.nodeStringValue(connectionSettings.get("hosts"), "localhost");
                String port =  XContentMapValues.nodeStringValue(connectionSettings.get("port"), "9160");
                String dataCentre =  XContentMapValues.nodeStringValue(connectionSettings.get("data_centre"), "UNKNOWN_DC");
                String username = XContentMapValues.nodeStringValue(connectionSettings.get("username"), "");
                String password = XContentMapValues.nodeStringValue(connectionSettings.get("password"), "");
                // init factory
                cassandraFactory = CassandraFactory.getInstance(hosts, port, dataCentre, username, password);
            }
            if(cassandraSettings != null && cassandraSettings.containsKey(SETTINGS_KEY_KEYSPACES))
            {
                LOGGER.debug("Parsing keyspace data");
                this.keyspaces = (List<Map<String, Object>>) cassandraSettings.get(SETTINGS_KEY_KEYSPACES);

            }
//            this.keySpace = XContentMapValues.nodeStringValue(cassandraSettings.get("keyspace"), "default");
//            this.columnFamily = XContentMapValues.nodeStringValue(cassandraSettings.get("column_family"), "unknown");
            if(cassandraSettings != null && cassandraSettings.containsKey(SETTINGS_KEY_SYNC))
            {
                LOGGER.debug("Parsing sync data");
                Map<String, Object> syncSettings = (Map<String, Object>) cassandraSettings.get(SETTINGS_KEY_CONNECTION);
                this.batchSize = XContentMapValues.nodeIntegerValue(syncSettings.get("batch_size"), 10000);
                this.schedule = XContentMapValues.nodeStringValue(syncSettings.get("cron"), "0/60 * * * * ?"); // DEFAULT every 60 second
            }
        }
//        if ( settings!=null &&  settings.settings().containsKey(SETTINGS_KEY_INDEX)) {
//            Map<String, Object> couchSettings = (Map<String, Object>) settings.settings().get("index");
//            this.indexName = XContentMapValues.nodeStringValue(couchSettings.get("index"), "DEFAULT_INDEX_NAME");
//            this.typeName = XContentMapValues.nodeStringValue(couchSettings.get("type"), "DEFAULT_TYPE_NAME");
//
//        } else {
//            this.indexName = "DEFAULT_INDEX_NAME";
//            this.typeName = "DEFAULT_TYPE_NAME";
//        }
    }

    public void start() {
        LOGGER.info("Starting Cassandra River");
        try {
            Scheduler scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.start();
            for (Map<String, Object> keyspace : this.keyspaces) {
                String keyspaceName = (String) keyspace.get("name");
                LOGGER.debug(String.format("processing %s keyspace", keyspaceName));
                // Job details including data
                JobDataMap jobData = new JobDataMap();
                jobData.put(JOB_DATA_KEY_KEYSPACE, keyspaceName);
                jobData.put(JOB_DATA_KEY_BATCH_SIZE, this.batchSize);
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> columnFamilies = (List<Map<String, Object>>) keyspace.get("column_families");
                for (Map<String, Object> columnFamily : columnFamilies) {
                    String columnFamilyName = (String) columnFamily.get("name");
                    LOGGER.debug(String.format("processing %s column family", columnFamilyName));
                    jobData.put(JOB_DATA_KEY_COLUMN_FAMILY, columnFamilyName);
                    List<String> primaryKey;
                    String tmp = XContentMapValues.nodeStringValue(columnFamily.get("primary_key"), "");
                    if(!tmp.isEmpty() && tmp.contains(",")) {
                        primaryKey = Arrays.asList(tmp.split(","));
                    } else {
                        primaryKey = new ArrayList<String>();
                        if(!tmp.isEmpty()) {
                            primaryKey.add(tmp);
                        }
                    }
                    jobData.put(JOB_DATA_KEY_PRIMARY_KEY, primaryKey);
                    @SuppressWarnings("unchecked")
                    Map<String,String> index = (Map<String, String>) columnFamily.get("index");
                    jobData.put(JOB_DATA_KEY_INDEX_NAME, index.get("name"));
                    jobData.put(JOB_DATA_KEY_INDEX_TYPE, index.get("type"));
                    String jobId = String.format("River %s %s Job", keyspaceName, columnFamilyName);
                    JobDetail jobDetail = JobBuilder.newJob(RiverJob.class)
                                                    .withIdentity(jobId,"river")
                                                    .setJobData(jobData)
                                                    .build();
                    //trigger
                    String triggerId = String.format("River %s %s Trigger", keyspaceName, columnFamilyName);
                    Trigger trigger = TriggerBuilder.newTrigger()
                            .withIdentity(triggerId, "river")
                            .startNow()
                            .withSchedule(CronScheduleBuilder.cronSchedule(this.schedule))
                            .build();
                    // schedule
                    scheduler.scheduleJob(jobDetail, trigger);
                }
            }
        } catch (SchedulerException e) {
            LOGGER.info("Scheduler Exception {}", e);
        }
        LOGGER.info("Cassandra River Started");
    }

    public void close() {
        LOGGER.info("Closing Cassandra River");
        getClient().admin().indices().prepareDeleteMapping("_river").setType("cassandra-river").execute();
        cassandraFactory.shutdown();
        LOGGER.info("Cassandra River Closed");
    }

    // Quartz Job
    @DisallowConcurrentExecution
    public static class RiverJob implements Job{
        public RiverJob() {     }

        public void execute(JobExecutionContext context) throws JobExecutionException {
            LOGGER.info(String.format("Processing %s Quartz Job", context.getFireInstanceId()));
            // Get input data
            JobDataMap jobData = context.getJobDetail().getJobDataMap();
            int batchSize = jobData.getInt(JOB_DATA_KEY_BATCH_SIZE);
            String keyspace = jobData.getString(JOB_DATA_KEY_KEYSPACE);
            String columnFamily = jobData.getString(JOB_DATA_KEY_COLUMN_FAMILY);
            @SuppressWarnings("unchecked")
            List<String> primaryKey = (List<String>) jobData.get(JOB_DATA_KEY_PRIMARY_KEY);
            String typeName = jobData.getString(JOB_DATA_KEY_INDEX_TYPE);
            String indexName = jobData.getString(JOB_DATA_KEY_INDEX_NAME);

            // executor to index
            ThreadFactory daemonThreadFactory = new ThreadFactoryBuilder().setNameFormat("Queue-Indexer-thread-%d").setDaemon(false).build();
            ExecutorService threadExecutor = Executors.newFixedThreadPool(20, daemonThreadFactory);

            Session session =  cassandraFactory.getSession(keyspace);

            String SQL = "select * from " + columnFamily +" ALLOW FILTERING;";
            PreparedStatement statement = session.prepare(SQL);

            BoundStatement bndStm = new BoundStatement(statement);
            bndStm.setFetchSize(batchSize);

            ResultSet resultSet = session.execute(bndStm.bind());
            Iterator<Row> result = resultSet.iterator();

            Map<String, Map<String, String>> values = new HashMap<String, Map<String, String>>();
            while(result.hasNext()){
                Row row = result.next();
                ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
                String primaryKeyValue = UUID.randomUUID().toString();
                if(!primaryKey.isEmpty()) {
                    LOGGER.debug("generating primary key from " + primaryKey);
                    primaryKeyValue = String.format("%s%s", keyspace, columnFamily);
                    for (Iterator<String> iterator = primaryKey.iterator(); iterator.hasNext();) {
                        String key = iterator.next();
                        DataType type = columnDefinitions.getType(key);
                        primaryKeyValue += CassandraFactory.getStringValue(type, row, key);
                    }
                    try {
                        MessageDigest crypt = MessageDigest.getInstance("SHA-1");
                        crypt.reset();
                        crypt.update(primaryKeyValue.getBytes("UTF-8"));
                        primaryKeyValue = new BigInteger(1, crypt.digest()).toString(16);
                    } catch(NoSuchAlgorithmException e) {
                        primaryKeyValue = UUID.randomUUID().toString();
                        LOGGER.warn("generating primary key from uuid " + primaryKeyValue + " as SHA-1 algorithm not found");
                    } catch(UnsupportedEncodingException e) {
                        primaryKeyValue = UUID.randomUUID().toString();
                        LOGGER.warn("generating primary key from uuid " + primaryKeyValue + " as UTF-8 is an unsupported encoding");
                    }
                } else {
                    LOGGER.debug("generating primary key from uuid " + primaryKeyValue);
                }
                Map<String, String> data = new HashMap<String, String>();

                for(int i = 0; i < columnDefinitions.size(); i++){
                    String columnName = columnDefinitions.getName(i);
                    String columnValue="";
                    DataType dataType = columnDefinitions.getType(i);

                    columnValue = CassandraFactory.getStringValue(dataType, row, columnName);

                    data.put(columnName, columnValue);
                }
                values.put(primaryKeyValue, data);
                if(values.size() >= batchSize){
                    // copy hash map
                    Map<String, Map<String, String>>  tmpValues = new HashMap<String, Map<String, String>>();
                    tmpValues.putAll(values);
                    threadExecutor.execute(new Indexer(batchSize, tmpValues, typeName, indexName));
                    values = new HashMap<String, Map<String, String>>();
                }
            }
            if(values.size() < batchSize){
                threadExecutor.execute(new Indexer(batchSize, values, typeName, indexName));
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

        public void run() {
            LOGGER.info("Starting thread with Data {}, batch size {}", this.keys.size(), this.batchSize);
            BulkRequestBuilder bulk = getClient().prepareBulk();
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

    public static Client getClient() {
        return client;
    }
}
