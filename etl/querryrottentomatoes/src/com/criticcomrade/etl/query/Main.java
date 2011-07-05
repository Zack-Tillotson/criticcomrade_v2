package com.criticcomrade.etl.query;

import java.io.IOException;
import java.sql.*;
import java.util.Date;

import com.criticcomrade.api.data.MovieShort;
import com.criticcomrade.api.main.RottenTomatoesApi;
import com.criticcomrade.etl.query.data.*;
import com.criticcomrade.etl.query.db.*;
import com.google.gson.JsonSyntaxException;

public class Main {
    
    private static final int STALE_TIME_PERIOD = 1000 * 60 * 60 * 24; // 1 Day
    private static final int ACTIVE_TIME_PERIOD_START = 1000 * 60 * 60 * 24 * 7; // 7 Day
    private static final int ACTIVE_TIME_PERIOD_END = 1000 * 60 * 60 * 24; // 1 Day
    
    private static Connection conn;
    
    public static void main(String[] args) throws JsonSyntaxException, IOException, SQLException {
	
	conn = DaoUtility.getConnection();
	
	RottenTomatoesApi api = new RottenTomatoesApi();
	for (MovieShort ms : api.getBoxOfficeMovies()) {
	    etlMovie(ms.id, STALE_TIME_PERIOD, api);
	}
	
//	for (MovieShort ms : api()) {
//	    etlMovie(ms.id, STALE_TIME_PERIOD);
//	}
//	
//	for (MovieShort ms : api()) {
//	    etlMovie(ms.id, STALE_TIME_PERIOD);
//	}
//	
//	for (MovieShort ms : api()) {
//	    etlMovie(ms.id, STALE_TIME_PERIOD);
//	}
//	
//	for (String id : RtQueueDao.getMoviesActiveWithinTimePeriod(new Date((new Date()).getTime() - ACTIVE_TIME_PERIOD_START),
//	        new Date((new Date()).getTime() - ACTIVE_TIME_PERIOD_END))) {
//	    etlMovie(id, STALE_TIME_PERIOD);
//	}
	
    }
    
    public static void etlMovie(String id, long staleTimePeriod, RottenTomatoesApi api) throws IOException {
	
	long startTime = System.currentTimeMillis();
	int apiCallCount = 0;
	
	RtQueueDao rtQueueDao = new RtQueueDao(conn);
	
	rtQueueDao.ensureMovieIsInQueue(id);
	
	Date nowDate = new Date();
	
	String result;
	if ((nowDate.getTime() - rtQueueDao.getLastQueryDate(id).getTime()) > staleTimePeriod) {
	    
	    try {
		
		RottenTomatoesMovieQuery mq = new RottenTomatoesMovieQuery(id, api);
		apiCallCount = mq.getApiCallCount();
		
		rtQueueDao.updateQueryDate(id, nowDate);
		
		boolean changed = (new DataItemDao(conn)).putDataItem(mq);
		if (changed) {
		    rtQueueDao.updateFoundDate(id, nowDate);
		}
		
		result = "Updated";
		
	    } catch (Exception e) {
		e.printStackTrace();
		result = e.toString();
	    }
	    
	} else {
	    result = "Not stale";
	}
	
	long endTime = System.currentTimeMillis();
	
	(new RtActivityDao(conn)).addApiCallToLog(id, result, apiCallCount, (int) ((endTime - startTime) / 1000));
	System.out.println(id + " " + result);
	
    }
    
    private static void printAttrsTree(String tab, DataItem item) {
	System.out.println(tab + item.getType() + " " + item.getId());
	for (Attribute attr : item.getAttributes()) {
	    System.out.println(String.format("%s%s = %s", tab, attr.attribute, attr.value));
	}
	System.out.println();
	for (DataItem dataItem : item.getSubItems()) {
	    printAttrsTree(tab + "\t", dataItem);
	}
    }
    
}