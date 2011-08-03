package com.criticcomrade.etl.query;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import com.criticcomrade.etl.RottenTomatoesEtlThread;
import com.criticcomrade.etl.data.DataItem;
import com.criticcomrade.etl.query.db.*;

public class RottenTomatoesFromQueueEtl extends RottenTomatoesEtlThread {
    
    public RottenTomatoesFromQueueEtl(Connection conn, int maxRuntime) throws AmbiguousQueryException {
	super(conn, maxRuntime);
    }
    
    @Override
    protected boolean haveReasonToQuit(List<String> reasons) {
	final Date nowWhen = new Date();
	boolean tooManyApiCalls = (new RtActivityDao(conn).getNumberOfApiCallsSince(new Date(nowWhen.getTime() - API_THROTTLE_PERIOD)) >= API_THROTTLE_AMOUNT);
	if (tooManyApiCalls) {
	    reasons.add("Too many API calls");
	}
	return tooManyApiCalls;
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
    @Override
    protected String getNextIdToEtl() {
	
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
    
    @Override
    public DataItem doEtlImpl(String id) {
	
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
		
	    } catch (IOException e) {
		if (e.toString().startsWith("java.io.IOException: Server returned HTTP response code: 500 for URL: ")) { // Bad Id
		    new RtQueueDao(conn).removeMovieFromQueue(id);
		    result = "Bad id, removed from queue";
		} else {
		    e.printStackTrace();
		    result = e.toString();
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
	System.out.println(String.format("%s %s %s", new SimpleDateFormat().format(new Date(endTime)), id, result));
	
	return mq;
	
    }
    
    public static void main(String args[]) throws SQLException, IOException, AmbiguousQueryException {
	
	RottenTomatoesFromQueueEtl o = new RottenTomatoesFromQueueEtl(DaoUtility.getConnection(), 1000 * 60 * 10);
	RottenTomatoesEtlThread.printAttrsTree("", o.doEtlImpl("770687943"));
	
    }
}
