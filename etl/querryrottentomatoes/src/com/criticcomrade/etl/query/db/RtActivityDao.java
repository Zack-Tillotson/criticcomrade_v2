package com.criticcomrade.etl.query.db;

import java.sql.*;
import java.util.Date;

public class RtActivityDao extends AbstractDao {
    
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
    
    public void addWebCallToLog(String id, String status, int webCalls, int durationInSectonds) {
	String sql = "insert into rt_scrape_activity (rt_id, status, estimated_web_calls, etl_duration_seconds) values (?, ?, ?, ?)";
	PreparedStatement statement;
	try {
	    statement = conn.prepareStatement(sql);
	    statement.setString(1, id);
	    statement.setString(2, status);
	    statement.setInt(3, webCalls);
	    statement.setInt(4, durationInSectonds);
	    statement.executeUpdate();
	    statement.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }
    
    public int getNumberOfApiCallsSince(Date when) {
	
	int ret = 0;
	
	String sql = "select sum(estimated_api_calls) from rt_activity where ts > ?";
	PreparedStatement statement;
	try {
	    statement = conn.prepareStatement(sql);
	    statement.setDate(1, new java.sql.Date(when.getTime()));
	    ResultSet rs = statement.executeQuery();
	    if (rs.next()) {
		ret = rs.getInt(1);
	    }
	    statement.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	
	return ret;
	
    }
    
}
