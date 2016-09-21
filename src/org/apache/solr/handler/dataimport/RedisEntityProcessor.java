package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

@SuppressWarnings("unused")
public class RedisEntityProcessor extends EntityProcessorBase {

    private static final String KEY = "key";
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private RedisDataSource dataSource;

    /**
     * Initializes the Redis entity processor
     *
     * @param context Solr context
     */
    @Override
    public void init(Context context) {
        super.init(context);
        this.dataSource = (RedisDataSource) context.getDataSource();
    }

    /**
     * Retrives the next key/value pair row from Redis
     *
     * @return The key/value pair row
     */
    @Override
    public Map<String, Object> nextRow() {
        String name = this.context.getEntityAttribute("name");
        LOG.debug(String.format("Running nextRow() for Entity: %s", name));
        if (rowIterator == null) {
            String query = this.context.getEntityAttribute(KEY);
            String replaced = this.context.replaceTokens(query);
            this.initQuery(replaced);
        }

        return getNext();
    }

    /**
     * Initializes and prepares the Redis key pattern
     *
     * @param query The Redis key pattern
     */
    private void initQuery(String query) {
        try {
            DataImporter.QUERY_COUNT.get().incrementAndGet();
            this.rowIterator = this.dataSource.getData(query);
            this.query = query;
        } catch (DataImportHandlerException e) {
            LOG.error(String.format("The Redis query failed '%s'", query), e);
            throw e;
        } catch (Exception e) {
            LOG.error(String.format("The Redis query failed '%s'", query), e);
            String message = String.format("Exception initializing query: %s", query);
            throw new DataImportHandlerException(SEVERE, message, e);
        }
    }
}
