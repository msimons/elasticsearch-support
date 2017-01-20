package org.xbib.elasticsearch.helper.client;


import java.util.HashMap;
import java.util.Map;

public class AcknowledgeMetric {
    private Map<Long, Boolean> results = new HashMap<>();

    public void addSucceeded(Long relatedJobId) {
        results.put(relatedJobId, true);
    }

    public void addFailed(Long relatedJobId) {
        results.put(relatedJobId, false);
    }

    public Map<Long, Boolean> getResult() {
        return results;
    }
}
