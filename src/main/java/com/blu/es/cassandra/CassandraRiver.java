package com.blu.es.cassandra;

import com.datastax.driver.core.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * User: bsha
 * Date: 08.07.2014
 * Time: 14:28
 */
public class CassandraRiver extends AbstractRiverComponent implements River {
    private static final String SETTINGS_KEY_CASSAANDRA="cassandra";

    private String hostName;
    private String dcName;
    private String port;
    private String clusterName;
    private String keySpace;
    private String columnFamily;
    private int batchSize;


    @Inject
    protected CassandraRiver(RiverName riverName, RiverSettings settings) {
        super(riverName, settings);
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
        while(ite.hasNext()){
            Row row = (Row) ite.next();
            logger.info("Row:" + row);
        }
        factory.shutdown();
    }

    @Override
    public void close() {
        logger.info("Cassandra River Close!!");
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

    public void setKeyspace(String keySpace) {
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
}
