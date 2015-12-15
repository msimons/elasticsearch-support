package org.xbib.elasticsearch.support.client;


import java.util.List;

public class AcknowledgeInfo {
    private Long actionIdMin;
    private Long actionIdMax;
    private boolean succeeded = false;
    private List<AcknowledgeItem> items;

    public AcknowledgeInfo(List<AcknowledgeItem> items) {
        this.items = items;
        calcDerivatives();
    }

    private void calcDerivatives() {
        Boolean s = null;
        Long aMin = null;
        Long aMax = null;

        if(processed()) {
            for (AcknowledgeItem item : items) {
                if (s == null || s) {
                    s = item.getJobIdentity().isSucceeded();
                }
                if (aMin == null || item.getJobIdentity().getId() < aMin) {
                    aMin = item.getJobIdentity().getId();
                }
                if (aMax == null || item.getJobIdentity().getId() > aMax) {
                    aMax = item.getJobIdentity().getId();
                }
            }
            this.actionIdMin = aMin;
            this.actionIdMax = aMax;
            this.succeeded = s;
        }
    }

    public boolean processed() {
        return items != null && items.size() > 0;
    }

    public List<AcknowledgeItem> getItems() {
        return items;
    }

    public Long getActionIdMin() {
        return actionIdMin;
    }

    public Long getActionIdMax() {
        return actionIdMax;
    }

    public boolean isSucceeded() {
        return succeeded;
    }
}
