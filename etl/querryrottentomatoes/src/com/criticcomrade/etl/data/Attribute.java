package com.criticcomrade.etl.data;

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
    
    @Override
    public boolean equals(Object o) {
	if (!(o instanceof Attribute)) {
	    return false;
	} else {
	    Attribute oa = (Attribute) o;
	    return (attribute != null) && (oa.attribute != null) && oa.attribute.equals(attribute) &&
		    (((value == null) && (oa.value == null)) || ((value != null) && oa.value.equals(value)));
	}
    }
    
    @Override
    public int hashCode() {
	return ((((attribute == null ? 0 : attribute.hashCode()) + (value == null ? 0 : value.hashCode()))) % Integer.MAX_VALUE);
    }
    
}
