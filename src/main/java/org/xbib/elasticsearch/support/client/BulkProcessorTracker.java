package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import java.util.*;

public class BulkProcessorTracker {
    private BulkProcessor bulkProcessor;
    private Map<DocumentIdentity,AckJobs> jobs = new HashMap<>();

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

        try {

            // do not track when jobid is null
            if (jobId == null) {
                return;
            }

            if (!jobs.containsKey(identity)) {
                jobs.put(identity,new AckJobs());
            }

            AckJobs ackJobs = jobs.get(identity);
            ackJobs.getJobs().add(new Job(jobId, 0));

        } catch (Exception e) {
            System.out.println("");
        }
    }

    public void succeeded(DocumentIdentity identity) {
        finish(identity,true);
    }

    public void failed(DocumentIdentity identity) {
        finish(identity,false);
    }

    private void finish(DocumentIdentity identity,boolean succeeded) {
        try {
            if (!jobs.containsKey(identity)) {
                return;
            }

            AckJobs ackJobs = jobs.get(identity);
            if(!succeeded) {
                ackJobs.jobFailed();
            }
        } catch(Exception e) {
         System.out.println("");
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
