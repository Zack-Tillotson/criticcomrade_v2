package com.criticcomrade.etl.query;

import java.sql.*;

public class RtQueueDao {
    
    public static Date getLastQueryDate(String id) {
	
	try {
	    
	    String sql = "insert into item_queue() values ()";
	    
	    PreparedStatement statement = DaoUtility.getConnection().prepareStatement(sql);
	    statement.executeUpdate();
	    
	    sql = "select LAST_INSERT_ID()";
	    statement = DaoUtility.getConnection().prepareStatement(sql);
	    ResultSet rs = statement.executeQuery();
	    
	    int id;
	    if (rs.next()) {
		id = rs.getInt(1);
	    } else {
		throw new SQLException("Did not correctly find the last inserted id");
	    }
	    
	    rs.close();
	    statement.close();
	    
	    return id;
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
}
