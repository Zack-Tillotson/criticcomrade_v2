package com.criticcomrade.etl.scrape;

import java.io.*;
import java.net.*;
import java.sql.Connection;
import java.util.*;

import com.criticcomrade.etl.data.*;
import com.criticcomrade.etl.query.AttributeConstants;
import com.criticcomrade.etl.query.db.*;

public class RottenTomatoesReviewsScrape extends Thread {
    
    private final Connection conn;
    List<DataItem> reviews;
    
    public RottenTomatoesReviewsScrape(Connection conn) {
	this.conn = conn;
    }
    
    @Override
    public void run() {
	movieReviewsScrape(""); // TODO
    }
    
    public int doWebCallsAndParse(String title) {
	
	int numPages = 1; // Updated by first page
	
	for (int i = 0; i < numPages; i++) {
	    
	    List<String> lines = getWebPage(buildUrl(title, i + 1));
	    
	    NumberOfPagesParser pageNumParser = (new NumberOfPagesParser(lines));
	    pageNumParser.parse();
	    try {
		numPages = pageNumParser.getObject();
	    } catch (ResultDoesNotExistException e) {
		e.printStackTrace();
		continue;
	    }
	    
	    MovieReviewParser pageReview = (new MovieReviewParser(lines));
	    while (pageReview.parse()) {
		try {
		    reviews.add(pageReview.getObject());
		} catch (ResultDoesNotExistException e) {
		    e.printStackTrace();
		    break;
		}
	    }
	    
	}
	
	return numPages;
	
    }
    
    private List<String> getWebPage(String urlString) {
	
	List<String> ret = new ArrayList<String>();
	
	URL url;
	InputStream is = null;
	DataInputStream dis;
	String line;
	try {
	    url = new URL(urlString);
	    is = url.openStream(); // throws an IOException
	    dis = new DataInputStream(new BufferedInputStream(is));
	    
	    while ((line = dis.readLine()) != null) {
		ret.add(line);
	    }
	} catch (MalformedURLException mue) {
	    mue.printStackTrace();
	} catch (IOException ioe) {
	    ioe.printStackTrace();
	} finally {
	    try {
		is.close();
	    } catch (IOException ioe) {
		// nothing to see here
	    }
	}
	
	return ret;
	
    }
    
    private String buildUrl(String title, int page) {
	return String.format("http://www.rottentomatoes.com/m/%s/reviews/?page=%d", title, page);
    }
    
    private void movieReviewsScrape(String id) {
	
	RtQueueDao rtQueueDao = new RtQueueDao(conn);
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
		String urlTitle = ""; // TODO 
		
		// Do the scrape
		webCallCount += doWebCallsAndParse(urlTitle);
		
		// For each review in the db, look for a matching review from the scrape. If it exists, add the +- review
		// and save it
		for (DataItem item : dataItemDao.findItemByAttributes(Arrays.asList(new Attribute(AttributeConstants.MOVIE_URL_TITLE, urlTitle))).getSubItems()) {
		    String val = item.getAttributeValue(AttributeConstants.REVIEW_LINK);
		    if (val != null) {
			for (DataItem review : reviews) {
			    if (val.equalsIgnoreCase(review.getAttributeValue(AttributeConstants.REVIEW_LINK))) {
				dataItemDao.setAttribute(item.getId(), AttributeConstants.REVIEW_IS_POSITIVE, review
				                .getAttributeValue(AttributeConstants.REVIEW_IS_POSITIVE));
				newReviewCount++;
			    }
			}
		    }
		}
		
		boolean changed = newReviewCount > 0;
		if (changed) {
		    rtQueueDao.updateFoundDate(id, nowDate);
		}
		
		rtQueueDao.updateQueryDate(id, nowDate);
		result = String.format("Updated %d reviews", newReviewCount);
		
	    } catch (Exception e) {
		result = e.toString();
	    } finally {
		rtQueueDao.removeMovieLock(id, rtQueueDao);
	    }
	}
	
	long endTime = System.currentTimeMillis();
	(new RtActivityDao(conn)).addWebCallToLog(id, result, webCallCount, (int) ((endTime - startTime) / 1000));
	System.out.println(String.format("%s %s", id, result));
	
    }
}
