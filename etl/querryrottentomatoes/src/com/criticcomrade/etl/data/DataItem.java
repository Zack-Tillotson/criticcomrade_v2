package com.criticcomrade.etl.data;

import java.util.*;

import com.criticcomrade.etl.query.AttributeConstants;

public abstract class DataItem {
    
    protected String type;
    protected int id;
    private Collection<DataItem> subItems;
    private final Collection<Attribute> addedAttrs = new ArrayList<Attribute>();
    
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
    
    public void addAttribute(Attribute a) {
	addedAttrs.add(a);
    }
    
    public Collection<Attribute> getAttributes() {
	
	Collection<Attribute> attrs = new ArrayList<Attribute>();
	attrs.add(new Attribute(AttributeConstants.TYPE, type));
	attrs.addAll(getDirectAttributes());
	attrs.addAll(addedAttrs);
	
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
    
}
