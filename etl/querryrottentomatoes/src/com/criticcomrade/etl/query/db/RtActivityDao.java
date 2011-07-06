package com.criticcomrade.etl.query.db;

import java.sql.*;

public class RtActivityDao extends EtlDao {
    
    public RtActivityDao(Connection conn) {
	super(conn);
    }
    
    public void addApiCallToLog(String id, String status, int apiCalls, int durationInSectonds) {
	String sql = "insert into rt_activity (rt_id, status, estimated_api_calls, etl_duration_seconds) values (?, ?, ?, ?)";
	PreparedStatement statement;
	try {
	    statement = conn.prepareStatement(sql);
	    statement.setString(1, id);
	    statement.setString(2, status);
	    statement.setInt(3, apiCalls);
	    statement.setInt(4, durationInSectonds);
	    statement.executeUpdate();
	    statement.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }
    
}