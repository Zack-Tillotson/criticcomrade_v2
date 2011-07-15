package com.criticcomrade.etl.query.db;

public class AmbiguousQueryException extends Exception {
    
    public AmbiguousQueryException(String msg) {
	super(msg);
    }
    
}
