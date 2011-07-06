package com.criticcomrade.etl.query.db;

import java.sql.Connection;

public class EtlDao {
    
    protected Connection conn;
    
    public EtlDao(Connection conn) {
	this.conn = conn;
    }
    
}
