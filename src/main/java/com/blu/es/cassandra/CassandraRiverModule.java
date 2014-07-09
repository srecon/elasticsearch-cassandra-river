package com.blu.es.cassandra;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 * User: bsha
 * Date: 08.07.2014
 * Time: 14:21
 */
public class CassandraRiverModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(River.class).to(CassandraRiver.class).asEagerSingleton();
    }
}
