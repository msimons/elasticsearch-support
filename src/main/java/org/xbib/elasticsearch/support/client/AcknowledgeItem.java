package org.xbib.elasticsearch.support.client;

public class AcknowledgeItem {
    private DocumentIdentity documentIdentity;
    private Job job;

    public AcknowledgeItem(DocumentIdentity documentIdentity, Job job) {
        this.documentIdentity = documentIdentity;
        this.job = job;
    }

    public DocumentIdentity getDocumentIdentity() {
        return documentIdentity;
    }

    public Job getJobIdentity() {
        return job;
    }

}
