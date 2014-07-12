elasticsearch-cassandra-river
=============================
Elasticsearch river for Cassandra 2.* with CQL support, based on data pull from Cassandra. Project Cloned and modified from the origin https://github.com/eBay/cassandra-river. 

1. Based on Datastax Java driver
2. CQL support
3. Support Cron Scheduling

##Setup
build : mvn clean install

install:

copy target/releases/cassandra-river-1.0-SNAPSHOT.zip into $ELASTICSEARCH_HOME/plugin/cassandra-river
  or
./plugin --url file:/river/cassandra-river-1.0-SNAPSHOT.zip --install cassandra-river

remove:
 ./plugin --remove cassandra-river

##Init:
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
##Search
Install plugin head
$ES_HOME\bin\plugin -install mobz/elasticsearch-head

Use Head plugin to search, you can download it from here
http://HOST:PORT/_plugin/head/


##Improvments
1. Tests
2. Update records
3. Add only changed row in ES  