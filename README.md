elasticsearch-cassandra-river
=============================
Elasticsearch river for Cassandra 2.* with CQL support.
Based on Datastax Java driver
to compile
mvn clean install

to install river
copy target/releases/cassandra-river-1.0-SNAPSHOT.zip into $ELASTICSEARCH_HOME/plugin/cassandra-river


Initilize river
curl -XPUT 'http://HOST:PORT/_river/cassandra_river/_meta' -d '{
    "type" : "cassandra",
    "cassandra" : {
        "cluster_name" : "CRM-MNP Cluster",
        "keyspace" : "mnpkeyspace",
        "column_family" : "event_log",
        "batch_size" : 1000,
        "hosts" : "192.168.202.115",
        "dcName" : "MNPANDC",
        "cron"  : "0/30 * * * * ?"
    },
    "index" : {
        "index" : "prodinfo",
        "type" : "product"
    }
}'
