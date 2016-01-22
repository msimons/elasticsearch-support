package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulkProcessorTracker {
    private BulkProcessor bulkProcessor;
    private Map<DocumentIdentity,AckJobs> jobs = new HashMap<>();

    public BulkProcessorTracker(BulkProcessor bulkProcessor) {
        this.bulkProcessor = bulkProcessor;
    }

    public BulkProcessor getDelegate() {
        return bulkProcessor;
    }

    public BulkProcessor add(IndexRequest request,Long... jobId) {
        trackJob(new DocumentIdentity(request),jobId);
        return bulkProcessor.add(request, null);
    }

    public BulkProcessor add(UpdateRequest request,Long... jobId) {
        trackJob(new DocumentIdentity(request),jobId);
        return bulkProcessor.add(request, null);
    }

    public BulkProcessor add(DeleteRequest request,Long... jobId) {
        trackJob(new DocumentIdentity(request),jobId);
        return bulkProcessor.add(request, null);
    }

    private void trackJob(DocumentIdentity identity,Long... jobId) {

        // do not track when jobid is null or length is zero
        if (jobId == null || jobId.length == 0) {
            return;
        }

        if (!jobs.containsKey(identity)) {
            jobs.put(identity,new AckJobs());
        }

        AckJobs ackJobs = jobs.get(identity);

        for(Long id : jobId) {
            ackJobs.getJobs().add(new Job(id, 0));
        }
    }

    public void succeeded(DocumentIdentity identity) {
        finish(identity,true);
    }

    public void failed(DocumentIdentity identity) {
        finish(identity,false);
    }

    private void finish(DocumentIdentity identity,boolean succeeded) {
        if (!jobs.containsKey(identity)) {
            return;
        }

        AckJobs ackJobs = jobs.get(identity);
        if(!succeeded) {
            ackJobs.jobFailed();
        }
    }

    public AcknowledgeInfo finish() {
        List<AcknowledgeItem> items = new ArrayList<>();
        for(DocumentIdentity i : jobs.keySet()) {
            AckJobs ackJobs = jobs.get(i);
            for(Job j : ackJobs.getJobs()) {
                AcknowledgeItem item = new AcknowledgeItem(i,j);
                item.getJobIdentity().setSucceeded(ackJobs.isSucceeded());
                items.add(item);
            }
        }

       AcknowledgeInfo acknowledgeInfo = new AcknowledgeInfo(items);

       jobs.clear();

       return acknowledgeInfo;
    }

    private class AckJobs {
        private List<Job> jobs= new ArrayList<>();
        private boolean succeeded = true;

        public void jobFailed() {
            succeeded = false;
        }

        public List<Job> getJobs() {
            return jobs;
        }

        public void setJobs(List<Job> jobs) {
            this.jobs = jobs;
        }

        public boolean isSucceeded() {
            return succeeded;
        }
    }


}
