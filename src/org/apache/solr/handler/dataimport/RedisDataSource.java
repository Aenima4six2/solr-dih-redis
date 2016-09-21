
package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.lang.invoke.MethodHandles;
import java.util.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;


public class RedisDataSource extends DataSource<Iterator<Map<String, Object>>> {

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String SSL = "ssl";
    private static final int DEFAULT_PORT = 6379;
    private static final boolean USE_SSL = false;
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private Jedis redis;

    @Override
    public void init(Context context, Properties initProps) {
        String host = initProps.getProperty(HOST);
        String port = initProps.getProperty(PORT);
        String ssl = initProps.getProperty(SSL);
        int targetPort = DEFAULT_PORT;
        boolean useSsl = USE_SSL;

        if (host == null || host.isEmpty()) {
            throw new DataImportHandlerException(SEVERE, "Redis host must be supplied");
        }

        if (port != null && !port.isEmpty() && tryParseInt(port)) {
            targetPort = Integer.parseInt(port);
        }

        if (ssl != null && !ssl.isEmpty() && tryParseBool(ssl)) {
            useSsl = Boolean.parseBoolean(ssl);
        }

        this.redis = new Jedis(host, targetPort, useSsl);
    }

    @Override
    public Iterator<Map<String, Object>> getData(final String query) {
        LOG.debug("Executing Redis Query: " + query);
        Set<Map<String, Object>> allKeys = new HashSet<>();
        for (String key : redis.keys(query)) {
            allKeys.addAll(this.getRow(key));
        }

        return allKeys.iterator();
    }

    @Override
    public void close() {
        if (this.redis != null) {
            this.redis.close();
        }
    }

    private Set<Map<String, Object>> getRow(String key) {
        Set<Map<String, Object>> results = new HashSet<>();
        String type = redis.type(key);
        switch (type) {
            case "string":
                String string = redis.get(key);
                HashMap<String, Object> stringResult = new HashMap<>();
                stringResult.put("key", key);
                stringResult.put("value", string);
                results.add(stringResult);
                break;
            case "list":
            case "set":
                for (String set : redis.smembers(key)) {
                    HashMap<String, Object> setResult = new HashMap<>();
                    setResult.put("key", key);
                    setResult.put("value", set);
                    results.add(setResult);
                }
                break;
        }

        return results;
    }

    private boolean tryParseInt(String value) {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean tryParseBool(String value) {
        try {
            Boolean.parseBoolean(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

}