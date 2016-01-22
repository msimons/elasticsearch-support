package org.xbib.elasticsearch.support.client.mock;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.support.client.AcknowledgeInfo;
import org.xbib.elasticsearch.support.client.transport.BulkTransportClient;

import java.io.IOException;
import java.util.Map;

/**
 * Mock client, it does not perform actions on a cluster.
 * Useful for testing or dry runs.
 */
public class MockTransportClient extends BulkTransportClient {

    @Override
    public AcknowledgeInfo acknowledge() {
        return null;
    }

    @Override
    public MockTransportClient newClient(Map<String,String> settings) {
        return this;
    }

    @Override
    public MockTransportClient newClient(Settings settings) {
        return this;
    }

    public Client client() {
        return null;
    }

    @Override
    public MockTransportClient maxActionsPerBulkRequest(int maxBulkActions) {
        return this;
    }

    @Override
    public MockTransportClient maxConcurrentBulkRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockTransportClient index(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockTransportClient delete(String index, String type, String id) {
        return this;
    }

    @Override
    public MockTransportClient update(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public BulkTransportClient bulkDelete(DeleteRequest deleteRequest, Long... jobId) {
        return this;
    }

    @Override
    public BulkTransportClient bulkIndex(IndexRequest indexRequest, Long... jobId) {
        return this;
    }

    @Override
    public BulkTransportClient bulkUpdate(UpdateRequest updateRequest, Long... jobId) {
        return this;
    }

    @Override
    public MockTransportClient flushIngest() {
        return this;
    }

    @Override
    public MockTransportClient waitForResponses(TimeValue timeValue) throws InterruptedException {
        return this;
    }

    @Override
    public MockTransportClient startBulk(String index, long startRefreshInterval, long stopRefreshIterval) {
        return this;
    }

    @Override
    public MockTransportClient stopBulk(String index) {
        return this;
    }

    @Override
    public MockTransportClient deleteIndex(String index) {
        return this;
    }

    @Override
    public MockTransportClient newIndex(String index) {
        return this;
    }

    @Override
    public MockTransportClient putMapping(String index) {
        return this;
    }

    @Override
    public MockTransportClient refreshIndex(String index) {
        return this;
    }

    @Override
    public MockTransportClient flushIndex(String index) {
        return this;
    }

    @Override
    public MockTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
        return this;
    }

    @Override
    public int waitForRecovery(String index) throws IOException {
        return -1;
    }

    @Override
    public int updateReplicaLevel(String index, int level) throws IOException {
        return -1;
    }

    @Override
    public void shutdown() {
        // do nothing
    }

}
