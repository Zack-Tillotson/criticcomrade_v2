package com.criticcomrade.etl.query;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;

import com.criticcomrade.api.data.MovieShort;
import com.criticcomrade.api.main.RottenTomatoesApi;
import com.criticcomrade.etl.query.data.*;
import com.criticcomrade.etl.query.db.*;
import com.google.gson.JsonSyntaxException;

public class Main extends Thread {
    
    private static final String PRINT_OPTIONS = "-?";
    private static final String FROM_QUEUE = "--from-queue";
    private static final String CURRENT_LISTS = "--from-current-lists";
    
    private static final int STALE_TIME_PERIOD = 1000 * 60 * 60 * 24; // 1 Day
    private static final int ACTIVE_TIME_PERIOD_START = 1000 * 60 * 60 * 24 * 7; // 7 Day
    private static final int ACTIVE_TIME_PERIOD_END = 1000 * 60 * 60 * 24; // 1 Day
    
    private static Connection conn;
    
    public static void main(String[] args) throws JsonSyntaxException, IOException, SQLException, InterruptedException {
	
	if (args.length != 1) {
	    printOptions();
	    System.exit(1);
	}
	
	conn = DaoUtility.getConnection();
	RottenTomatoesApi api = new RottenTomatoesApi();
	
	if (args[0].equals(CURRENT_LISTS)) {
	    
	    for (MovieShort ms : api.getBoxOfficeMovies()) {
		doMovieEtl(ms.id, api);
	    }
	    
	    for (MovieShort ms : api.getInTheatersMovies()) {
		doMovieEtl(ms.id, api);
	    }
	    
	    for (MovieShort ms : api.getOpeningMovies()) {
		doMovieEtl(ms.id, api);
	    }
	    
	    for (MovieShort ms : api.getUpcomingMovies()) {
		doMovieEtl(ms.id, api);
	    }
	    
	} else if (args[0].equals(FROM_QUEUE)) {
	    
	    while (true) {
		
		String id = getNextInQueueToEtl();
		
		// If we didn't find any movies we want to query, just sleep for a bit so we can try again later
		if (id == null) {
		    sleep(1000 * 60 * 10);
		} else {
		    doMovieEtl(id, api);
		}
	    }
	    
	} else {
	    printOptions();
	    System.exit(1);
	}
	
    }
    
    /**
     * Will return the id of the next movie on the queue which should be ETL'd. This movie will not
     * be locked (at the time of this call). The order movies will be returned are: active movies
     * (movies with a found date within the active window but a last query date outside the stale
     * window), then the newest movie on the queue with a null last queried date, then the movie on
     * the queue with the oldest last querried date
     * 
     * @return A RottenTomatoes movie ID to ETL
     */
    private static String getNextInQueueToEtl() {
	
	final RtQueueDao rtQueueDao = new RtQueueDao(conn);
	
	/////////////////
	
	List<String> activeMovies = rtQueueDao.getMovieActiveWithinTimePeriod(new Date((new Date()).getTime() - ACTIVE_TIME_PERIOD_START),
	                new Date((new Date()).getTime() - ACTIVE_TIME_PERIOD_END));
	
	if (activeMovies.size() > 0) {
	    return activeMovies.get(0);
	}
	
	/////////////////
	
	List<String> newMovies = rtQueueDao.getMovieNoQueryDate();
	
	if (newMovies.size() > 0) {
	    return newMovies.get(0);
	}
	
	/////////////////
	
	List<String> staleMovies = rtQueueDao.getMovieLeastRecentlyQueried();
	
	if (staleMovies.size() > 0) {
	    return staleMovies.get(0);
	}
	
	return null;
	
    }
    
    private static void printOptions() {
	System.err.println("Usage: options");
	System.err.println("\t" + PRINT_OPTIONS + "\tPrint these options");
	System.err.println("\t" + CURRENT_LISTS + "\tETL the current box office, in theaters, opening, and upcoming movies from RottenTomatoes");
	System.err.println("\t" + FROM_QUEUE + "\tETL any movies off of the queue which are \"active\"");
    }
    
    public static void doMovieEtl(String id, RottenTomatoesApi api) throws IOException {
	
	long startTime = System.currentTimeMillis();
	int apiCallCount = 0;
	RtQueueDao rtQueueDao = new RtQueueDao(conn);
	Date nowDate = new Date();
	String result;
	
	if (!RtQueueDao.getMovieLock(id, rtQueueDao)) {
	    result = "Locked";
	} else {
	    
	    try {
		
		if ((nowDate.getTime() - rtQueueDao.getLastQueryDate(id).getTime()) > STALE_TIME_PERIOD) {
		    
		    try {
			
			RottenTomatoesMovieQuery mq = new RottenTomatoesMovieQuery(id, api);
			apiCallCount = mq.getApiCallCount();
			
			boolean changed = (new DataItemDao(conn)).putDataItem(mq);
			if (changed) {
			    rtQueueDao.updateFoundDate(id, nowDate);
			}
			
			result = "Updated";
			
		    } catch (Exception e) {
			e.printStackTrace();
			result = e.toString();
		    }
		    
		    rtQueueDao.updateQueryDate(id, nowDate);
		    
		} else {
		    result = "Not stale";
		}
		
	    } finally {
		RtQueueDao.removeMovieLock(id, rtQueueDao);
	    }
	}
	
	long endTime = System.currentTimeMillis();
	(new RtActivityDao(conn)).addApiCallToLog(id, result, apiCallCount, (int) ((endTime - startTime) / 1000));
	System.out.println(String.format("%s %s", id, result));
	
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