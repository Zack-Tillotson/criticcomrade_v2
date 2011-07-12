package com.criticcomrade.etl.scrape;

import java.sql.*;
import java.util.*;
import java.util.Date;

import com.criticcomrade.etl.data.*;
import com.criticcomrade.etl.query.*;
import com.criticcomrade.etl.query.db.*;

public class RottenTomatoesReviewsScrape extends Thread {
    
    private static final int ACTIVE_TIME_PERIOD_START = 1000 * 60 * 60 * 24 * 7; // 7 Day
    private static final int ACTIVE_TIME_PERIOD_END = 1000 * 60 * 60 * 24; // 1 Day
    
    private final Connection conn;
    
    public RottenTomatoesReviewsScrape(Connection conn) {
	this.conn = conn;
    }
    
    @Override
    public void run() {
	try {
	    while (true) {
		
		String id = getNextInQueueToScrape();
		
		// If we didn't find any movies we want to scrape, just sleep for a bit so we can try again later
		if (id == null) {
		    sleep(1000 * 60 * 10);
		} else {
		    scrapeMovieReviews(id);
		    sleep(new Random().nextInt(10 * 1000)); // Don't be mean
		}
	    }
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
    }
    
    /**
     * Will return the id of the next movie on the queue which should be scraped. This movie will
     * not be locked (at the time of this call). Movies will be returned by finding reviews which
     * don't have a plus/minus score for any of the
     * 
     * @return A RottenTomatoes movie ID to ETL
     */
    public String getNextInQueueToScrape() {
	
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
    
    public DataItem scrapeMovieReviews(String id) {
	
	DataItem ret = null;
	
	final RtQueueDao rtQueueDao = new RtQueueDao(conn);
	final DataItemDao dataItemDao = new DataItemDao(conn);
	
	long startTime = System.currentTimeMillis();
	int webCallCount = 0;
	int newReviewCount = 0;
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
			RottenTomatoesWebScrape scrape = new RottenTomatoesWebScrape(conn, urlTitle);
			webCallCount = scrape.getNumPages();
			
			// For each review in the db, look for a matching review from the scrape. If it exists, add the +- review
			// and save it
			final Collection<DataItem> dbSubItems = movieDataItem.getSubItems();
			for (DataItem item : dbSubItems) {
			    String val = item.getAttributeValue(AttributeConstants.REVIEW_LINK);
			    if (val != null) {
				for (DataItem review : scrape.getReviews()) {
				    if (val.equalsIgnoreCase(review.getAttributeValue(AttributeConstants.REVIEW_LINK))) {
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
			
			result = String.format("Updated %d reviews", newReviewCount);
			
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
	System.out.println(String.format("%s %s", id, result));
	
	return ret;
	
    }
    
    public static void main(String[] args) throws SQLException {
	
	RottenTomatoesReviewsScrape o = new RottenTomatoesReviewsScrape(DaoUtility.getConnection());
	RottenTomatoesFromQueueEtl.printAttrsTree("", o.scrapeMovieReviews("771208514"));
	
    }
    
}
