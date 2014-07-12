elasticsearch-cassandra-river
=============================
Elasticsearch river for Cassandra 2.* version with CQL support, based on data pull method from Cassandra cluster. Project Cloned and modified from the origin https://github.com/eBay/cassandra-river. 

1. Based on Datastax Java driver
2. CQL support
3. Support Cron Scheduling

##Setup
build : mvn clean install

install:

- copy target/releases/cassandra-river-1.0-SNAPSHOT.zip into $ELASTICSEARCH_HOME/plugin/cassandra-river
  or
- ./plugin --url file:/river/cassandra-river-1.0-SNAPSHOT.zip --install cassandra-river

remove:
 ./plugin --remove cassandra-river

##Init:
    curl -XPUT 'http://HOST:PORT/_river/cassandra-river/_meta' -d '{
        "type" : "cassandra",
        "cassandra" : {
            "cluster_name" : "Test Cluster",
            "keyspace" : "nortpole",
            "column_family" : "users",
            "batch_size" : 20000,
            "hosts" : "localhost",
            "dcName" : "DC",
            "cron"  : "0/60 * * * * ?"
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
1. Add unit Tests
2. Update records
3. Add newly added rows in ES by date
4. Add multi tables support  