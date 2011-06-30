package com.criticcomrade.etl.query.data;

import java.util.*;

import com.criticcomrade.etl.query.AttributeConstants;

public abstract class DataItem {
    
    protected String type;
    
    public DataItem(String type) {
	this.type = type;
    }
    
    public Collection<Attribute> getAttributes() {
	Collection<Attribute> attrs = new ArrayList<Attribute>();
	attrs.add(new Attribute(AttributeConstants.TYPE, type));
	attrs.addAll(getDirectAttributes());
	return attrs;
    }
    
    protected abstract Collection<Attribute> getDirectAttributes();
    
    public abstract Collection<DataItem> getSubItems();
    
}
