package com.blu.es.plugin.river;

import com.blu.es.cassandra.CassandraRiverModule;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

/**
 * User: bsha
 * Date: 08.07.2014
 * Time: 14:17
 */
public class CassandraRiverPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "cassandra-river";
    }

    @Override
    public String description() {
        return "Cassandra river plugin.";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("cassandra", CassandraRiverModule.class);
    }
}
