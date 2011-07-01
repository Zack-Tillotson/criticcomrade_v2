package com.criticcomrade.etl.query;

import java.io.*;
import java.sql.*;
import java.util.*;

public class DaoUtility {
    
    private static String userName;
    private static String password;
    private static String serverName;
    private static String portNumber;
    private static String database;
    
    private static void loadProperties() {
	Scanner in = null;
	try {
	    in = new Scanner(new File("db.properties"));
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	}
	
	Map<String, String> props = new HashMap<String, String>();
	while (in.hasNext()) {
	    String[] items = in.nextLine().split("=");
	    props.put(items[0], items[1]);
	}
	
	userName = props.get("username");
	password = props.get("password");
	serverName = props.get("servername");
	portNumber = props.get("serverport");
	database = props.get("database");
	
    }
    
    private static Connection conn = null;
    
    public static Connection getConnection() throws SQLException {
	if (conn == null) {
	    loadProperties();
	    Properties connectionProps = new Properties();
	    connectionProps.put("user", userName);
	    connectionProps.put("password", password);
	    
	    conn = DriverManager.getConnection("jdbc:mysql://" + serverName + ":" + portNumber + "/" + database, connectionProps);
	}
	return conn;
    }
    
}
