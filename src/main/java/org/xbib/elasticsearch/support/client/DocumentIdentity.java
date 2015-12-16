package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

public class DocumentIdentity {
    private String type;
    private String id;

    public DocumentIdentity(String type, String id) {
        this.type = type;
        this.id = id;
    }

    public DocumentIdentity(IndexRequest indexRequest) {
        this.type = indexRequest.type();
        this.id = indexRequest.id();
    }

    public DocumentIdentity(UpdateRequest updateRequest) {
        this.type = updateRequest.type();
        this.id = updateRequest.id();
    }

    public DocumentIdentity(DeleteRequest deleteRequest) {
        this.type = deleteRequest.type();
        this.id = deleteRequest.id();
    }

    public String getType() {
        return type;
    }
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DocumentIdentity that = (DocumentIdentity) o;

        if (!id.equals(that.id)) return false;
        if (!type.equals(that.type)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + id.hashCode();
        return result;
    }
}