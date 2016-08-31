package org.xbib.elasticsearch.helper.client;


import org.elasticsearch.action.bulk.BulkItemResponse;

import java.util.HashMap;
import java.util.Map;

public class AcknowledgeMetric {
    private Map<DocumentIdentity,Boolean> results = new HashMap<>();

    public void addSucceeded(BulkItemResponse response) {
        results.put(new DocumentIdentity(response),true);
    }

    public void addFailed(BulkItemResponse response) {
        results.put(new DocumentIdentity(response),false);
    }

    public Map<DocumentIdentity, Boolean> getResult() {
        return results;
    }
}
