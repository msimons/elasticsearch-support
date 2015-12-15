package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

public class DocumentIdentity {
    private String index;
    private String type;
    private String id;

    public DocumentIdentity(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    public DocumentIdentity(IndexRequest indexRequest) {
        this.index = indexRequest.index();
        this.type = indexRequest.type();
        this.id = indexRequest.id();
    }

    public DocumentIdentity(UpdateRequest updateRequest) {
        this.index = updateRequest.index();
        this.type = updateRequest.type();
        this.id = updateRequest.id();
    }

    public DocumentIdentity(DeleteRequest deleteRequest) {
        this.index = deleteRequest.index();
        this.type = deleteRequest.type();
        this.id = deleteRequest.id();
    }

    public String getIndex() {
        return index;
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

        DocumentIdentity documentIdentity = (DocumentIdentity) o;

        if (!id.equals(documentIdentity.id)) return false;
        if (!index.equals(documentIdentity.index)) return false;
        if (!type.equals(documentIdentity.type)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + id.hashCode();
        return result;
    }
}