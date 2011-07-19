package com.criticcomrade.etl.query.db;

import java.sql.*;

public class RtControllerDao extends AbstractDao {
    
    public RtControllerDao(Connection conn) {
	super(conn);
    }
    
    public int getCurrentRunName() throws AmbiguousQueryException {
	
	try {
	    
	    StringBuilder sql = new StringBuilder("select max(name) from etl_controller");
	    PreparedStatement statement = conn.prepareStatement(sql.toString());
	    
	    ResultSet rs = statement.executeQuery();
	    
	    int ret;
	    try {
		if (rs.next()) {
		    ret = rs.getInt(1);
		} else {
		    throw new AmbiguousQueryException("Couldn't find name of current run");
		}
	    } finally {
		rs.close();
		statement.close();
	    }
	    
	    return ret;
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
	
    }
    
    public int nextRun() {
	
	synchronized (this) {
	    
	    try {
		
		StringBuilder sql = new StringBuilder("insert into etl_controller() values ()");
		PreparedStatement statement = conn.prepareStatement(sql.toString());
		
		statement.executeUpdate();
		statement.close();
		
		sql = new StringBuilder("select last_insert_id()");
		statement = conn.prepareStatement(sql.toString());
		
		ResultSet rs = statement.executeQuery();
		int ret;
		try {
		    rs.next();
		    ret = rs.getInt(1);
		} finally {
		    rs.close();
		    statement.close();
		}
		
		return ret;
		
	    } catch (SQLException e) {
		throw new RuntimeException(e);
	    }
	    
	}
	
    }
    
}
