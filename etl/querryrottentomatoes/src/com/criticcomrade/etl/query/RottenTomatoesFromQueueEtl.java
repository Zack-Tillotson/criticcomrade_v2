package com.criticcomrade.etl.query;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;

import com.criticcomrade.etl.data.*;
import com.criticcomrade.etl.query.db.*;

public class RottenTomatoesFromQueueEtl extends Thread {
    
    private static final int ACTIVE_TIME_PERIOD_START = 1000 * 60 * 60 * 24 * 7; // 7 Day
    private static final int ACTIVE_TIME_PERIOD_END = 1000 * 60 * 60 * 24; // 1 Day
    private static final int STALE_TIME_PERIOD = 1000 * 60 * 60 * 24 * 30; // 1 Month
    private static final int API_THROTTLE_PERIOD = 1000 * 60 * 60 * 24; // 1 Day
    private static final int API_THROTTLE_AMOUNT = 8000 + new Random().nextInt(1000); // Don't do more than 9000 calls a day
    private final Connection conn;
    
    private final Date startWhen = new Date();
    
    public RottenTomatoesFromQueueEtl(Connection conn) {
	this.conn = conn;
    }
    
    @Override
    public void run() {
	
	try {
	    String id;
	    while (shouldContinueToEtl() && ((id = getNextInQueueToEtl()) != null)) {
		doMovieEtl(id);
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
	
    }
    
    /**
     * Will quit after the set throttle period, and make sure we don't overuse the API during that
     * time
     * 
     * @return
     */
    private boolean shouldContinueToEtl() {
	final Date nowWhen = new Date((new Date()).getTime() - API_THROTTLE_PERIOD);
	return ((nowWhen.getTime() - startWhen.getTime()) < API_THROTTLE_PERIOD) &&
	        (new RtActivityDao(conn).getNumberOfApiCallsSince(nowWhen) < API_THROTTLE_AMOUNT);
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
    private String getNextInQueueToEtl() {
	
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
	
	List<String> staleMovies = rtQueueDao.getMovieIsStale(new Date((new Date()).getTime() - STALE_TIME_PERIOD));
	
	if (staleMovies.size() > 0) {
	    return staleMovies.get(0);
	}
	
	return null;
	
    }
    
    public DataItem doMovieEtl(String id) throws IOException {
	
	RottenTomatoesMovieQuery mq = null;
	
	RtQueueDao rtQueueDao = new RtQueueDao(conn);
	final DataItemDao dataItemDao = new DataItemDao(conn);
	
	long startTime = System.currentTimeMillis();
	int apiCallCount = 0;
	Date nowDate = new Date();
	String result;
	
	if (!RtQueueDao.getMovieLock(id, rtQueueDao)) {
	    result = "Locked";
	} else {
	    
	    try {
		
		mq = new RottenTomatoesMovieQuery(id);
		apiCallCount = mq.getApiCallCount();
		
		boolean changed = dataItemDao.putDataItem(mq);
		if (changed) {
		    rtQueueDao.updateFoundDate(id, nowDate);
		    result = "Updated";
		} else {
		    result = "No change";
		}
		
	    } catch (Exception e) {
		e.printStackTrace();
		result = e.toString();
	    } finally {
		rtQueueDao.updateQueryDate(id, nowDate);
		rtQueueDao.removeMovieLock(id, rtQueueDao);
	    }
	}
	
	long endTime = System.currentTimeMillis();
	(new RtActivityDao(conn)).addApiCallToLog(id, result, apiCallCount, (int) ((endTime - startTime) / 1000));
	System.out.println(String.format("%s %s", id, result));
	
	return mq;
	
    }
    
    public static void printAttrsTree(String tab, DataItem item) {
	System.out.println(tab + item.getType() + " " + item.getId());
	for (Attribute attr : item.getAttributes()) {
	    System.out.println(String.format("%s%s = %s", tab, attr.attribute, attr.value));
	}
	System.out.println();
	for (DataItem dataItem : item.getSubItems()) {
	    printAttrsTree(tab + "\t", dataItem);
	}
    }
    
    public static void main(String args[]) throws SQLException, IOException {
	
	RottenTomatoesFromQueueEtl o = new RottenTomatoesFromQueueEtl(DaoUtility.getConnection());
	RottenTomatoesFromQueueEtl.printAttrsTree("", o.doMovieEtl("770687943"));
	
    }
}
