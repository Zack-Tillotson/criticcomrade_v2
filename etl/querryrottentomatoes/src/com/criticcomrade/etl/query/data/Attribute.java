package com.criticcomrade.etl.query.data;

public class Attribute {
    
    public final String attribute;
    public final String value;
    
    public Attribute(String attribute, String value) {
	this.attribute = attribute;
	this.value = value;
    }
    
    @Override
    public String toString() {
	return String.format("%s = %s", attribute, value);
    }
    
}
