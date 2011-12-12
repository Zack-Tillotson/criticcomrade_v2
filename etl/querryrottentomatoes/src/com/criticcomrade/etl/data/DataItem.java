package com.criticcomrade.etl.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.criticcomrade.etl.query.AttributeConstants;

public abstract class DataItem {

    protected String type;
    protected int id;
    private Collection<DataItem> subItems;

    public DataItem(String type) {
        this.type = type;
        subItems = null;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public Collection<Attribute> getAttributes() {

        Collection<Attribute> attrs = new ArrayList<Attribute>();
        attrs.addAll(getDirectAttributes());

        Collection<Attribute> extraAttrs = new ArrayList<Attribute>();
        extraAttrs.add(new Attribute(AttributeConstants.TYPE, type));
        for (DataItem subitem : getSubItems()) {
            extraAttrs.add(new Attribute(subitem.getType(), String.format("%d", subitem.getId())));
        }

        // Make sure we have the extra items
        for (Attribute extraAttr : extraAttrs) {
            if (!attrs.contains(extraAttr)) {
                attrs.add(extraAttr);
            }
        }

        // Remove nulls
        for (Iterator<Attribute> iter = attrs.iterator(); iter.hasNext();) {
            if (iter.next().value == null) {
                iter.remove();
            }
        }

        return attrs;

    }

    public String getAttributeValue(String attrName) {

        for (Attribute attr : getAttributes()) {
            if (attr.attribute.equals(attrName)) {
                return attr.value;
            }
        }
        return null;

    }

    protected abstract Collection<Attribute> getDirectAttributes();

    public Collection<DataItem> getSubItems() {
        if (subItems == null) {
            subItems = buildSubItems();
            if (subItems == null) {
                subItems = new ArrayList<DataItem>();
            }
        }

        return subItems;
    }

    protected abstract Collection<DataItem> buildSubItems();

    public List<Integer> getAllItemIds() {

        List<Integer> ret = new ArrayList<Integer>();

        ret.add(getId());
        for (DataItem item : getSubItems()) {
            ret.addAll(item.getAllItemIds());
        }

        return ret;

    }

}
