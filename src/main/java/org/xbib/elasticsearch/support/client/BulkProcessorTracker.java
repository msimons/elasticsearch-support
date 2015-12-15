package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import java.util.*;

public class BulkProcessorTracker {
    private BulkProcessor bulkProcessor;
    private Map<DocumentIdentity,Queue<Job>> jobs = new HashMap<>();

    public BulkProcessorTracker(BulkProcessor bulkProcessor) {
        this.bulkProcessor = bulkProcessor;
    }

    public BulkProcessor getDelegate() {
        return bulkProcessor;
    }

    public BulkProcessor add(Long jobId, IndexRequest request) {
        trackJob(jobId, new DocumentIdentity(request));
        return bulkProcessor.add(request, null);
    }

    public BulkProcessor add(Long jobId, UpdateRequest request) {
        trackJob(jobId, new DocumentIdentity(request));
        return bulkProcessor.add(request, null);
    }

    public BulkProcessor add(Long jobId, DeleteRequest request) {
        trackJob(jobId, new DocumentIdentity(request));
        return bulkProcessor.add(request, null);
    }

    private void trackJob(Long jobId, DocumentIdentity identity) {

        // do not track when jobid is null
        if(jobId == null) {
            return;
        }

        if(!jobs.containsKey(identity)) {
            Queue<Job> queue = new LinkedList<>();
            queue.add(new Job(jobId,0));
            jobs.put(identity, queue);
            return;
        }

        Queue<Job> queue = jobs.get(identity);
        Job last = queue.peek();
        queue.add(new Job(jobId,last.getOrder() + 1));
    }

    public void succeeded(DocumentIdentity identity) {
        finish(identity,true);
    }

    public void failed(DocumentIdentity identity) {
        finish(identity,false);
    }

    private void finish(DocumentIdentity identity,boolean succeeded) {
        if(!jobs.containsKey(identity)){
            return;
        }

        Queue<Job> queue = jobs.get(identity);
        Job job = queue.peek();

        if(job == null) {
            return;
        }

        job.setSucceeded(succeeded);
    }

    public AcknowledgeInfo finish() {
        List<AcknowledgeItem> items = new ArrayList<>();
        for(DocumentIdentity i : jobs.keySet()) {
            boolean allSucceeded = true;
            for(Job a : jobs.get(i)) {
                if(!a.isSucceeded()) {
                    allSucceeded = false;
                    break;
                }
            }
            for(Job a : jobs.get(i)) {
                AcknowledgeItem item = new AcknowledgeItem(i,a);
                item.getJobIdentity().setSucceeded(allSucceeded);
                items.add(item);
            }
        }

       AcknowledgeInfo acknowledgeInfo = new AcknowledgeInfo(items);

       jobs.clear();

       return acknowledgeInfo;
    }


}
