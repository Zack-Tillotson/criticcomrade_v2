package com.criticcomrade.etl.query.db;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class RtQueueDao extends AbstractDao {
    
    public RtQueueDao(Connection conn) {
	super(conn);
    }
    
    public static synchronized boolean getMovieLock(String id, RtQueueDao dao) {
	dao.ensureMovieIsInQueue(id);
	return dao.lockMovie(id);
    }
    
    public void removeMovieLock(String id, RtQueueDao dao) {
	dao.unlockMovie(id);
    }
    
    /**
     * Syncronized by getMovieLock
     * 
     * @param id
     * @return
     */
    private boolean lockMovie(String id) {
	
	try {
	    
	    String sql = "select date_locked from rt_queue where rt_id = ?";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
	    statement.setString(1, id);
	    ResultSet rs = statement.executeQuery();
	    
	    if (rs.next()) {
		String lock = rs.getString(1);
		if (lock != null) {
		    return false;
		}
	    }
	    
	    rs.close();
	    statement.close();
	    
	    sql = "update rt_queue set date_locked = ? where rt_id = ?";
	    statement = conn.prepareStatement(sql);
	    statement.setTimestamp(1, new Timestamp(new Date().getTime()));
	    statement.setString(2, id);
	    statement.executeUpdate();
	    
	    rs.close();
	    statement.close();
	    
	    return true;
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    /**
     * Syncronized by removeMovieLock
     * 
     * @param id
     * @return
     */
    private void unlockMovie(String id) {
	
	try {
	    
	    String sql = "update rt_queue set date_locked = null where rt_id = ?";
	    PreparedStatement statement = conn.prepareStatement(sql);
	    statement.setString(1, id);
	    statement.executeUpdate();
	    
	    statement.close();
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    private void ensureMovieIsInQueue(String id) {
	
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
		statement = conn.prepareStatement(sql);
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
	    statement.setTimestamp(1, when == null ? null : new java.sql.Timestamp(when.getTime()));
	    statement.setString(2, id);
	    statement.executeUpdate();
	    
	    statement.close();
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    public void updateScrapeDate(String id, Date when) {
	
	try {
	    
	    String sql = "update rt_queue set date_last_scraped = ? where rt_id = ?";
	    
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
    
    public List<String> getMovieActiveWithinTimePeriod(Date start, Date end) {
	
	try {
	    
	    String sql = "select rt_id from rt_queue where date_locked is null and date_last_found > ? and (date_last_queried is null or date_last_queried < ?) order by date_last_found desc limit 1";
	    
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
    
    /**
     * Will return a movie with at least one review which is active since the start of the period,
     * but not scraped since the end
     * 
     * @param start
     * @param end
     * @return
     */
    public List<String> getMovieScrapeActiveWithinTimePeriod(Date start, Date end) {
	
	try {
	    
	    String sql = "select rt_id from rt_queue, data d, data dd where date_locked is null and date_last_found > ? and (date_last_scraped is null or date_last_scraped < ?) and d.attr_value = rt_id and d.item_id = dd.item_id and dd.attr_name = 'REVIEW' order by date_last_found asc limit 1";
	    
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
    
    public List<String> getMovieWithNoScrapeDate() {
	
	try {
	    
	    String sql = "select rt_id from rt_queue, data d, data dd where date_locked is null and date_last_scraped is null and date_last_queried is not null and d.attr_value = rt_id and d.item_id = dd.item_id and dd.attr_name = 'REVIEW' order by rt_id desc limit 1";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
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
    
    public List<String> getMovieNoQueryDate() {
	
	try {
	    
	    String sql = "select rt_id from rt_queue where date_locked is null and date_last_queried is null order by rt_id desc limit 1";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
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
    
    /**
     * Gets a movie which was last querried before the stale date
     * 
     * @param stale The date movies should have been last queried before to be considered stale
     * @return
     */
    public List<String> getMovieIsStale(Date stale) {
	
	try {
	    
	    String sql = "select rt_id from rt_queue where date_locked is null and date_last_queried is not null and date_last_queried < ? order by date_last_queried asc limit 1";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
	    statement.setTimestamp(1, new java.sql.Timestamp(stale.getTime()));
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
    
    public void setAsActive(String id) {
	
	try {
	    
	    String sql = "update rt_queue set date_last_found = ? where rt_id = ?";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
	    statement.setTimestamp(1, new java.sql.Timestamp(new Date().getTime() - 1000 * 60 * 60 * 24));
	    statement.setString(2, id);
	    int changedCount = statement.executeUpdate();
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    public boolean removeMovieFromQueue(String id) {
	try {
	    
	    String sql = "delete from rt_queue where rt_id = ?";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
	    statement.setString(1, id);
	    
	    int changedCount = statement.executeUpdate();
	    
	    return changedCount == 1;
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
}
