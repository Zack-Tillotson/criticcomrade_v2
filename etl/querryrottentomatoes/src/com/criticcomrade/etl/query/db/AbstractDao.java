package com.criticcomrade.etl.query.db;

import java.sql.Connection;

public class AbstractDao {
    
    protected Connection conn;
    
    public AbstractDao(Connection conn) {
	this.conn = conn;
    }
    
}
