package com.criticcomrade.etl.query.db;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class RtQueueDao extends EtlDao {
    
    public RtQueueDao(Connection conn) {
	super(conn);
    }
    
    public void ensureMovieIsInQueue(String id) {
	
	try {
	    
	    String sql = "select * from rt_queue where rt_id = ?";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
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
    
    public Date getLastQueryDate(String id) {
	
	try {
	    
	    String sql = "select date_last_queried from rt_queue where rt_id = ?";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
	    statement.setString(1, id);
	    ResultSet rs = statement.executeQuery();
	    
	    Date ret = null;
	    if (rs.next()) {
		ret = rs.getTimestamp(1);
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
    
    public void updateQueryDate(String id, Date when) {
	
	try {
	    
	    String sql = "update rt_queue set date_last_queried = ? where rt_id = ?";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
	    statement.setTimestamp(1, new java.sql.Timestamp(when.getTime()));
	    statement.setString(2, id);
	    statement.executeUpdate();
	    
	    statement.close();
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    public void updateFoundDate(String id, Date when) {
	
	try {
	    
	    String sql = "update rt_queue set date_last_found = ? where rt_id = ?";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
	    statement.setTimestamp(1, new java.sql.Timestamp(when.getTime()));
	    statement.setString(2, id);
	    statement.executeUpdate();
	    
	    statement.close();
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    public List<String> getMoviesActiveWithinTimePeriod(Date start, Date end) {
	
	try {
	    
	    String sql = "select rt_id from rt_queue where date_last_found > ? and date_last_found < ? order by date_last_found desc";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
	    statement.setTimestamp(1, new Timestamp(start.getTime()));
	    statement.setTimestamp(2, new Timestamp(end.getTime()));
	    ResultSet rs = statement.executeQuery();
	    
	    List<String> ret = new ArrayList<String>();
	    while (rs.next()) {
		ret.add(rs.getString(1));
	    }
	    
	    rs.close();
	    statement.close();
	    
	    return ret;
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
}
