package com.criticcomrade.etl.query.data;

import java.util.*;

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
	attrs.add(new Attribute(AttributeConstants.TYPE, type));
	attrs.addAll(getDirectAttributes());
	
	// Remove nulls
	for (Iterator<Attribute> iter = attrs.iterator(); iter.hasNext();) {
	    if (iter.next().value == null) {
		iter.remove();
	    }
	}
	
	return attrs;
    }
    
    protected abstract Collection<Attribute> getDirectAttributes();
    
    public Collection<DataItem> getSubItems() {
	if (subItems == null) {
	    subItems = buildSubItems();
	}
	
	return subItems;
    }
    
    protected abstract Collection<DataItem> buildSubItems();
    
}
