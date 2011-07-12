package com.criticcomrade.etl.scrape;

import java.io.*;
import java.net.*;
import java.sql.Connection;
import java.util.*;

import com.criticcomrade.etl.data.DataItem;

public class RottenTomatoesWebScrape {
    
    final public Connection conn;
    
    private List<DataItem> reviews;
    private int numPages = 0;
    
    public RottenTomatoesWebScrape(Connection conn, String url) {
	this.conn = conn;
	doWebCallsAndParse(url);
    }
    
    private void doWebCallsAndParse(String title) {
	
	reviews = new ArrayList<DataItem>();
	numPages = 1; // Updated by first page
	boolean haveRetried = false;
	
	for (int i = 0; i < numPages; i++) {
	    
	    List<String> lines = getWebPage(buildUrl(title, i + 1));
	    
	    NumberOfPagesParser pageNumParser = (new NumberOfPagesParser(lines));
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
	
    }
    
    public List<DataItem> getReviews() {
	return reviews;
    }
    
    public int getNumPages() {
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
}