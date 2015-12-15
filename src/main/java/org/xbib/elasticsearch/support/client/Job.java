package org.xbib.elasticsearch.support.client;

public class Job {
    private Long id;
    private Integer order;
    private boolean succeeded = false;

    public Job(Long id, Integer order){
        this.id = id;
        this.order = order;
    }

    public Long getId() {
        return id;
    }

    public Integer getOrder() {
        return order;
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public void setSucceeded(boolean succeeded) {
        this.succeeded = succeeded;
    }
}