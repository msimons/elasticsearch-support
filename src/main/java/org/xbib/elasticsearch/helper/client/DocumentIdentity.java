package org.xbib.elasticsearch.helper.client;


import org.elasticsearch.action.bulk.BulkItemResponse;

public class DocumentIdentity {
    private String index;
    private String type;
    private String id;
    private String optype;

    public DocumentIdentity(BulkItemResponse response) {
        this.index = response.getIndex();
        this.type = response.getType();
        this.id = response.getId();
        this.optype = response.getOpType();

    }

    public DocumentIdentity(String index, String type, String id, String optype) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.optype = optype;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DocumentIdentity that = (DocumentIdentity) o;

        if (index != null ? !index.equals(that.index) : that.index != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (!id.equals(that.id)) return false;
        return optype.equals(that.optype);

    }

    @Override
    public int hashCode() {
        int result = index != null ? index.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + id.hashCode();
        result = 31 * result + optype.hashCode();
        return result;
    }
}
