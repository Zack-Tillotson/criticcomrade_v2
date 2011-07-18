package com.criticcomrade.etl.scrape;

import java.io.IOException;
import java.sql.Connection;
import java.util.*;

import com.criticcomrade.etl.data.DataItem;

public class RottenTomatoesWebScraper {
    
    final public Connection conn;
    
    private List<DataItem> reviews;
    private int numPages = 0;
    
    public RottenTomatoesWebScraper(Connection conn, String url) {
	this.conn = conn;
	try {
	    doWebCallsAndParse(url);
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }
    
    private void doWebCallsAndParse(String title) throws IOException {
	
	reviews = new ArrayList<DataItem>();
	numPages = 1; // Updated by first page
	boolean haveRetried = false;
	
	for (int i = 0; i < numPages; i++) {
	    
	    WebPageGetter in = new WebPageGetter(buildUrl(title, i + 1));
	    
	    try {
		
		NumberOfPagesParser pageNumParser = (new NumberOfPagesParser(in));
		pageNumParser.parse();
		try {
		    numPages = pageNumParser.getObject();
		    if (numPages == 0) {
			return;
		    }
		} catch (ResultDoesNotExistException e) {
		    if (!haveRetried) {
			haveRetried = true;
			i--;
		    } else {
			e.printStackTrace();
		    }
		    continue;
		}
		
		MovieReviewParser pageReview = (new MovieReviewParser(in));
		while (pageReview.parse()) {
		    try {
			reviews.add(pageReview.getObject());
		    } catch (ResultDoesNotExistException e) {
			e.printStackTrace();
			break;
		    }
		}
		
	    } finally {
		in.closeDataInputStream();
	    }
	    
	}
	
    }
    
    public List<DataItem> getReviews() {
	return reviews;
    }
    
    public int getNumPages() {
	return numPages;
    }
    
    private String buildUrl(String title, int page) {
	return String.format("http://www.rottentomatoes.com/m/%s/reviews/?page=%d", title, page);
    }
}