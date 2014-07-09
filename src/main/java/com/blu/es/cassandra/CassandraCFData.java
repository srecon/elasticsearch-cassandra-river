package com.blu.es.cassandra;

import java.util.HashMap;
import java.util.Map;

/**
 * User: bsha
 * Date: 09.07.2014
 * Time: 12:02
 */
public class CassandraCFData {
    private Map<String, Map<String, String>> data = new HashMap<String, Map<String, String>>();

    public Map<String, Map<String, String>> getData() {
        return data;
    }
}
