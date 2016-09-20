package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

public class RedisEntityProcessor extends EntityProcessorBase {

    private static final String KEY = "key";
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private RedisDataSource dataSource;

    @Override
    public void init(Context context) {
        super.init(context);
        this.dataSource = (RedisDataSource) context.getDataSource();
    }

    @Override
    public Map<String, Object> nextRow() {
        LOG.info("Running nextRow for Entity: " + this.context.getEntityAttribute("name"));
        if (rowIterator == null) {
            String query = this.context.getEntityAttribute(KEY);
            String replaced = this.context.replaceTokens(query);
            this.initQuery(replaced);
        }

        return getNext();
    }

    private void initQuery(String query) {
        try {
            DataImporter.QUERY_COUNT.get().incrementAndGet();
            this.rowIterator = this.dataSource.getData(query);
            this.query = query;
        } catch (DataImportHandlerException e) {
            LOG.error("The query failed '" + query + "'", e);
            throw e;
        } catch (Exception e) {
            LOG.error("The query failed '" + query + "'", e);
            wrapAndThrow(SEVERE, e, "Exception initializing query: " + query);
        }
    }
}
