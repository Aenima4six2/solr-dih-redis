
package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.lang.invoke.MethodHandles;
import java.util.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;


public class RedisDataSource extends DataSource<Iterator<Map<String, Object>>> {

    private static final String REDIS_URI = "url";
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private Jedis redis;

    @Override
    public void init(Context context, Properties initProps) {
        String url = initProps.getProperty(REDIS_URI);
        if (url == null) {
            throw new DataImportHandlerException(SEVERE, "Redis URL must be supplied");
        }

        this.redis = new Jedis(url);
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

}