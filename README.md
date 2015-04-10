elasticsearch-cassandra-river
=============================
Elasticsearch river for Cassandra 2.* version with CQL support, based on data pull method from Cassandra cluster. Project Cloned and modified from the origin https://github.com/eBay/cassandra-river.

1. Based on Datastax Java driver
2. CQL support
3. Support Cron Scheduling

##Setup
build : mvn clean install

install:

- copy target/releases/cassandra-river-1.0.6-SNAPSHOT.zip into $ELASTICSEARCH_HOME/plugin/cassandra-river
  or
- ./plugin --url file:/river/cassandra-river-1.0.6-SNAPSHOT.zip --install cassandra-river

remove:
 ./plugin --remove cassandra-river

##Init:
    curl -XPUT 'http://HOST:PORT/_river/cassandra-river/_meta' -d '{
        "type" : "cassandra",
        "cassandra" : {
            "connection" :
            {
                "hosts" : "hostname",
                "data_centre" : "dc",
                "username" : "optional_username",
                "password" : "optional_password"
            },
            "sync" :
            {
                "batch_size" : 20000,
                "schedule" : "0 0/15 * * * ?"
            },
            "keyspaces" :
            [
                {
                    "name" : "keyspace_name",
                    "column_families" :
                    [
                        {
                            "name" : "column_family_name",
                            "primary_key" : "column_family_primary_key",
                            "index" :
                            {
                                "name" : "keyspace_name_column_family_name_index",
                                "type" : "keyspace_name_column_family_name"
                            },
                            "columns" :
                            [
                                {
                                    "name" : "user_id",
                                    "type" : "string"
                                    "raw": "true"
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    }'

Notes on the above:

 * `hosts` can be a single ip address or dns name or comma separated list (with no spaces).
 * `keyspaces` is a list of dictionaries describing the keyspaces and their column familes.
 * `column_families` is a list of dictionaries describing the column family.
 * `column_families` -> `index` optional attribute to set the index name and type values to be used to create the index.
 * `column_families` -> `columns` optional attribute to describe column mapping characteristics.
 * `column_families` -> `columns` -> `name` name of the column
 * `column_families` -> `columns` -> `type` one of the core types: string, integer/long, float/double, boolean and null.
 * `column_families` -> `columns` -> `raw` optional flag that indecates that a raw field should be created as well, useful for sorting a column/attribute that is searchable as well.

##Search
Install plugin head
$ES_HOME\bin\plugin -install mobz/elasticsearch-head

Use Head plugin to search, you can download it from here
http://HOST:PORT/_plugin/head/


##Improvments
1. Add unit Tests
2. Add newly added rows in ES by date
