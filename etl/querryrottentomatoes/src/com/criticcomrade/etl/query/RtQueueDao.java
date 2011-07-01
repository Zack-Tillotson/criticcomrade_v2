package com.criticcomrade.etl.query;

import java.sql.*;
import java.util.Date;

public class RtQueueDao {
    
    public static void ensureMovieIsInQueue(String id) {
	
	try {
	    
	    String sql = "select * from rt_queue where rt_id = ?";
	    
	    PreparedStatement statement = DaoUtility.getConnection().prepareStatement(sql);
	    statement.setString(1, id);
	    ResultSet rs = statement.executeQuery();
	    
	    if (rs.next()) {
	    } else {
		
		rs.close();
		statement.close();
		
		sql = "insert into rt_queue (rt_id, link) values (?, ?)";
		statement = DaoUtility.getConnection().prepareStatement(sql);
		statement.setString(1, id);
		statement.setString(2, "http://api.rottentomatoes.com/api/public/v1.0/movies/" + id + ".json");
		statement.executeUpdate();
		
	    }
	    
	    rs.close();
	    statement.close();
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    public static Date getLastQueryDate(String id) {
	
	try {
	    
	    String sql = "select last_queried from rt_queue where rt_id = ?";
	    
	    PreparedStatement statement = DaoUtility.getConnection().prepareStatement(sql);
	    statement.setString(1, id);
	    ResultSet rs = statement.executeQuery();
	    
	    Date ret = null;
	    if (rs.next()) {
		ret = rs.getDate(1);
	    }
	    
	    rs.close();
	    statement.close();
	    
	    if (ret == null) {
		ret = new Date(0);
	    }
	    
	    return ret;
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    public static void updateQueryDate(String id, Date when) {
	
	try {
	    
	    String sql = "update rt_queue set last_queried = ? where rt_id = ?";
	    
	    PreparedStatement statement = DaoUtility.getConnection().prepareStatement(sql);
	    statement.setDate(1, new java.sql.Date(when.getTime()));
	    statement.setString(2, id);
	    statement.executeUpdate();
	    
	    statement.close();
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    public static void updateFoundDate(String id, Date when) {
	
	try {
	    
	    String sql = "update rt_queue set last_found = ? where rt_id = ?";
	    
	    PreparedStatement statement = DaoUtility.getConnection().prepareStatement(sql);
	    statement.setDate(1, new java.sql.Date(when.getTime()));
	    statement.setString(2, id);
	    statement.executeUpdate();
	    
	    statement.close();
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
}
