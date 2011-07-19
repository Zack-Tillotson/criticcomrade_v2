package com.criticcomrade.etl.scrape;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import com.criticcomrade.etl.data.*;
import com.criticcomrade.etl.query.*;
import com.criticcomrade.etl.query.db.*;

public class RottenTomatoesFromWebEtl extends RottenTomatoesEtlThread {
    
    public RottenTomatoesFromWebEtl(Connection conn, int maxRuntime) throws AmbiguousQueryException {
	super(conn, maxRuntime);
    }
    
    @Override
    protected boolean haveReasonToQuit() {
	return false;
    }
    
    /**
     * Will return the id of the next movie on the queue which should be scraped. This movie will
     * not be locked (at the time of this call). Movies will be returned by finding reviews which
     * don't have a plus/minus score for any of the
     * 
     * @return A RottenTomatoes movie ID to ETL
     */
    @Override
    protected String getNextIdToEtl() {
	
	final RtQueueDao rtQueueDao = new RtQueueDao(conn);
	
	/////////////////
	
	List<String> activeMovies = rtQueueDao.getMovieScrapeActiveWithinTimePeriod(new Date((new Date()).getTime() - ACTIVE_TIME_PERIOD_START),
	                new Date((new Date()).getTime() - ACTIVE_TIME_PERIOD_END));
	
	if (activeMovies.size() > 0) {
	    return activeMovies.get(0);
	}
	
	/////////////////
	
	List<String> newMovies = rtQueueDao.getMovieWithNoScrapeDate();
	
	if (newMovies.size() > 0) {
	    return newMovies.get(0);
	}
	
	return null;
	
    }
    
    @Override
    public DataItem doEtlImpl(String id) {
	
	DataItem ret = null;
	
	final RtQueueDao rtQueueDao = new RtQueueDao(conn);
	final DataItemDao dataItemDao = new DataItemDao(conn);
	
	long startTime = System.currentTimeMillis();
	int webCallCount = 0;
	int newReviewCount = 0;
	int totReviewCount = 0;
	Date nowDate = new Date();
	String result;
	
	if (!RtQueueDao.getMovieLock(id, rtQueueDao)) {
	    result = "Locked";
	} else {
	    
	    try {
		
		// Lookup the title
		final DataItem movieDataItem = dataItemDao.findItemByAttributes(Arrays.asList(new Attribute(AttributeConstants.MOVIE_ID, id)));
		if (movieDataItem == null) {
		    result = "Movie item not yet querried";
		} else {
		    
		    ret = movieDataItem;
		    
		    String urlTitle = movieDataItem.getAttributeValue(AttributeConstants.MOVIE_URL_TITLE);
		    
		    if (urlTitle == null) {
			result = "No url title in data item";
		    } else {
			
			// Do the scrape
			rtQueueDao.updateScrapeDate(id, nowDate);
			RottenTomatoesWebScraper scrape = new RottenTomatoesWebScraper(conn, urlTitle);
			webCallCount = scrape.getNumPages();
			
			// For each review in the db, look for a matching review from the scrape. If it exists, add the +- review
			// and save it
			final Collection<DataItem> dbSubItems = movieDataItem.getSubItems();
			for (DataItem item : dbSubItems) {
			    String val = item.getAttributeValue(AttributeConstants.REVIEW_LINK);
			    if (val != null) {
				totReviewCount++;
				for (DataItem review : scrape.getReviews()) {
				    if (val.equalsIgnoreCase(review.getAttributeValue(AttributeConstants.REVIEW_LINK))) {
					
					// CONSIDER Refactor this so that you just add the attribute to the item and put the item like normal
					if (dataItemDao.setAttribute(item.getId(), AttributeConstants.REVIEW_IS_POSITIVE, review
					        .getAttributeValue(AttributeConstants.REVIEW_IS_POSITIVE))) {
					    newReviewCount++;
					}
					
				    }
				}
			    }
			}
			
			boolean changed = newReviewCount > 0;
			if (changed) {
			    rtQueueDao.updateFoundDate(id, nowDate);
			}
			
			result = String.format("Updated %d of %d reviews", newReviewCount, totReviewCount);
			
		    }
		}
		
	    } catch (Exception e) {
		e.printStackTrace();
		result = e.toString();
	    } finally {
		rtQueueDao.removeMovieLock(id, rtQueueDao);
	    }
	}
	
	long endTime = System.currentTimeMillis();
	(new RtActivityDao(conn)).addWebCallToLog(id, result, webCallCount, (int) ((endTime - startTime) / 1000));
	System.out.println(String.format("%s %s %s", new SimpleDateFormat().format(new Date(endTime)), id, result));
	
	return ret;
	
    }
    
    public static void main(String[] args) throws SQLException, AmbiguousQueryException {
	
	RottenTomatoesFromWebEtl o = new RottenTomatoesFromWebEtl(DaoUtility.getConnection(), 10);
	RottenTomatoesEtlThread.printAttrsTree("", o.doEtlImpl("770687943"));
	
    }
    
}
