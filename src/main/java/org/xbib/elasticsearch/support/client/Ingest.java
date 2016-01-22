package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Interface for providing convenient administrative methods for ingesting data into Elasticsearch.
 */
public interface Ingest {

    /**
     * Index document
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return this
     */
    Ingest index(String index, String type, String id, String source);


    /**
     * Acknowledge info
     * @return Acknowledgeinfo
     */
    AcknowledgeInfo acknowledge() throws InterruptedException;

    /**
     * Delete document
     *
     * @param index the index
     * @param type  the type
     * @param id    the id
     * @return this
     */
    Ingest delete(String index, String type, String id);

    /**
     * Update document
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return this
     */
    Ingest update(String index, String type, String id, String source);

    Ingest newClient(Client client) throws IOException;

    Ingest newClient(Settings settings) throws IOException;

    Ingest newClient(Map<String,String> settings) throws IOException;

    Client client();

    /**
     * Set the maximum number of actions per bulk request
     *
     * @param maxActions maximum number of bulk actions
     * @return this ingest
     */
    Ingest maxActionsPerBulkRequest(int maxActions);

    /**
     * Set the maximum concurent bulk requests
     *
     * @param maxConcurentBulkRequests maximum number of concurrent ingest requests
     * @return this Ingest
     */
    Ingest maxConcurrentBulkRequests(int maxConcurentBulkRequests);

    /**
     * Set the maximum volume for bulk request before flush
     *
     * @param maxVolume maximum volume
     * @return this ingest
     */
    Ingest maxVolumePerBulkRequest(ByteSizeValue maxVolume);

    /**
     * Set the flush interval for automatic flushing outstanding ingest requests
     *
     * @param flushInterval the flush interval, default is 30 seconds
     * @return this ingest
     */
    Ingest flushIngestInterval(TimeValue flushInterval);

    //void setSettings(Settings settings);

    //Settings getSettings();

    ImmutableSettings.Builder getSettingsBuilder();

    /**
     * Create settings
     *
     * @param in the input stream with settings
     */
    void setting(InputStream in) throws IOException;

    /**
     * Create a key/value in the settings
     *
     * @param key   the key
     * @param value the value
     */
    void addSetting(String key, String value);

    /**
     * Create a key/value in the settings
     *
     * @param key   the key
     * @param value the value
     */
    void addSetting(String key, Boolean value);

    /**
     * Create a key/value in the settings
     *
     * @param key   the key
     * @param value the value
     */
    void addSetting(String key, Integer value);

    void mapping(String type, InputStream in) throws IOException;

    void mapping(String type, String mapping) throws IOException;

    Map<String, String> getMappings();

    Ingest putMapping(String index);

    Ingest deleteMapping(String index, String type);

    /**
     * Create a new index
     *
     * @return this ingest
     */
    Ingest newIndex(String index);

    Ingest newIndex(String index, String type, InputStream settings, InputStream mappings) throws IOException;

    Ingest newIndex(String index, Settings settings, Map<String,String> mappings);

    /**
     * Create new mapping
     * @param index index
     * @param type index type
     * @param mapping mapping
     * @return this ingest
     */
    Ingest newMapping(String index, String type, Map<String,Object> mapping);

    /**
     * Delete index
     *
     * @return this ingest
     */
    Ingest deleteIndex(String index);

    /**
     * Start bulk mode
     *
     * @return this ingest
     */
    Ingest startBulk(String index, long startRefreshInterval, long stopRefreshInterval) throws IOException;

    /**
     * Stops bulk mode. Enables refresh.
     *
     * @return this Ingest
     */
    Ingest stopBulk(String index) throws IOException;

    /**
     * Bulked index request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     * @param indexRequest the index request to add
     * @param jobId Id for external correlation
     * @return this ingest
     */
    Ingest bulkIndex(IndexRequest indexRequest,Long... jobId);

    /**
     * Bulked delete request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     * @param deleteRequest the delete request to add
     * @param jobId Id for external correlation
     * @return this ingest
     */
    Ingest bulkDelete(DeleteRequest deleteRequest,Long... jobId);

    /**
     * Bulked update request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     * @param updateRequest the update request to add
     * @param jobId Id for external correlation
     * @return this ingest
     */
    Ingest bulkUpdate(UpdateRequest updateRequest,Long... jobId);



    /**
     * Flush ingest, move all pending documents to the bulk indexer
     *
     * @return this
     */
    Ingest flushIngest();

    /**
     * Wait for all outstanding responses
     *
     * @param maxWait maximum wait time
     * @return this ingest
     * @throws InterruptedException
     */
    Ingest waitForResponses(TimeValue maxWait) throws InterruptedException;

    /**
     * Refresh the index.
     *
     * @return this ingest
     */
    Ingest refreshIndex(String index);

    Ingest flushIndex(String index);

    /**
     * Add replica level.
     *
     * @param level the replica level
     * @return number of shards after updating replica level
     */
    int updateReplicaLevel(String index, int level) throws IOException;

    /**
     * Wait for cluster being healthy.
     *
     * @throws IOException
     */
    Ingest waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException;

    /**
     * Wait for index recovery (after replica change)
     *
     * @return number of shards found
     */
    int waitForRecovery(String index) throws IOException;

    Ingest setMetric(Metric metric);

    Metric getMetric();

    boolean hasThrowable();

    /**
     * Return last throwable if exists.
     *
     * @return last throwable
     */
    Throwable getThrowable();

    /**
     * Shutdown the ingesting
     */
    void shutdown();
}
