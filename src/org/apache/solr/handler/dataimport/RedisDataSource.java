
package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.lang.invoke.MethodHandles;
import java.util.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;


@SuppressWarnings("WeakerAccess")
public class RedisDataSource extends DataSource<Iterator<Map<String, Object>>> {

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String SSL = "ssl";
    private static final int DEFAULT_PORT = 6379;
    private static final boolean USE_SSL = false;
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private Jedis redis;

    /**
     * Initializes the Redis data source
     *
     * @param context   Solr context
     * @param initProps Solr initialization properties
     */
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

        String name = context.getEntityAttribute("name");
        String msg = "Creating a Redis connection for entity %s: host %s - port %s - ssl - %s";
        LOG.info(String.format(msg, name, host, port, ssl));
        this.redis = new Jedis(host, targetPort, useSsl);
    }

    /**
     * Retrieves data from the Redis connection source
     *
     * @param query the Redis key pattern to retrieve keys
     * @return A row iterator
     */
    @Override
    public Iterator<Map<String, Object>> getData(final String query) {
        LOG.debug(String.format("Executing Redis Query: %s", query));
        Set<Map<String, Object>> rows = new HashSet<>();
        for (String key : redis.keys(query)) {
            rows.addAll(this.createRows(key));
        }

        return rows.iterator();
    }

    /**
     * Closes the underlying Redis connection
     */
    @Override
    public void close() {
        if (this.redis != null) {
            this.redis.close();
        }
    }

    /**
     * Create a set of one column rows for each Key/Value pair
     *
     * @param key the Redis Key
     * @return The set of Rows
     */
    private Set<Map<String, Object>> createRows(String key) {
        Set<Map<String, Object>> results = new HashSet<>();
        String type = redis.type(key);
        LOG.debug(String.format("Key %s is type: %s", key, type));

        switch (type) {
            case "string":
                // Treat the individual value as a row
                String stringValue = redis.get(key);
                results.add(createRow(key, stringValue));
                LOG.debug(String.format("Key %s: %s", key, stringValue));
                break;
            case "list":
            case "set":
                // Treat each value in the pair as a new row
                for (String setValue : redis.smembers(key)) {
                    results.add(createRow(key, setValue));
                    LOG.debug(String.format("Key %s: %s", key, setValue));
                }
                break;
        }

        return results;
    }

    /**
     * Create a one column row for each Key/Value Pair
     *
     * @param key   the Redis Key
     * @param value the value for the Redis Key
     * @return The Row
     */
    private Map<String, Object> createRow(String key, Object value) {
        HashMap<String, Object> row = new HashMap<>();
        row.put("key", key);
        row.put("value", value);
        return row;
    }

    /**
     * Tries to parse a string to an integer
     *
     * @param value the integer stored as a string
     * @return True if the value can be parsed
     */
    private boolean tryParseInt(String value) {
        try {
            //noinspection ResultOfMethodCallIgnored
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Tries to parse a string to a boolean
     *
     * @param value the boolean stored as a string
     * @return True if the value can be parsed
     */
    private boolean tryParseBool(String value) {
        try {
            //noinspection ResultOfMethodCallIgnored
            Boolean.parseBoolean(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

}