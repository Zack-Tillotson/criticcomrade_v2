package com.criticcomrade.etl.scrape;

import java.util.*;
import java.util.regex.*;

import com.criticcomrade.etl.data.*;
import com.criticcomrade.etl.query.AttributeConstants;

public class MovieReviewParser extends StringListParser<DataItem> {
    
    private enum Status {
	BEFORE_REVIEW, BEFORE_TMETERFIELDS, AT_SCORE, BEFORE_LINK
    }
    
    public MovieReviewParser(List<String> lines) {
	super(lines);
    }
    
    private Boolean positiveReview = null;
    
    @Override
    protected DataItem parseImpl() {
	
	Pattern p = null;
	Matcher m = null;
	
	Status status = Status.BEFORE_REVIEW;
	
	while (isMoreLinesExist()) {
	    
	    String line = getNextLine();
	    
	    switch (status) {
		case BEFORE_REVIEW:

		    p = Pattern.compile("<div\\s+class=\"criticinfo\"");
		    m = p.matcher(line);
		    if (m.find()) {
			status = Status.BEFORE_TMETERFIELDS;
		    }
		    
		    break;
		case BEFORE_TMETERFIELDS:

		    p = Pattern.compile("<\\s*div\\s+class=\"tmeterfield\\s*\"\\s*>");
		    m = p.matcher(line);
		    
		    if (m.find()) {
			status = Status.AT_SCORE;
		    }
		    
		    break;
		case AT_SCORE:

		    p = Pattern.compile("<\\s*div.*(fresh|rotten)");
		    m = p.matcher(line);
		    
		    if (m.find()) {
			status = Status.BEFORE_LINK;
			positiveReview = m.group(1).equalsIgnoreCase("fresh");
		    }
		    
		    break;
		case BEFORE_LINK:

		    p = Pattern.compile("<a\\s+href=\"([^\"]*)\".*>\\s*Full\\s+Review");
		    m = p.matcher(line);
		    
		    if (m.find()) {
			final String link = m.group(1);
			return new DataItem(AttributeConstants.REVIEW) {
			    @Override
			    protected Collection<Attribute> getDirectAttributes() {
				Collection<Attribute> attrs = new ArrayList<Attribute>();
				attrs.add(new Attribute(AttributeConstants.REVIEW_IS_POSITIVE, positiveReview ? "TRUE" : "FALSE"));
				attrs.add(new Attribute(AttributeConstants.REVIEW_LINK, link));
				return attrs;
			    }
			    
			    @Override
			    protected Collection<DataItem> buildSubItems() {
				return new ArrayList<DataItem>();
			    }
			};
		    }
		    
		    break;
		
	    }
	    
	}
	
	return null;
	
    }
    
}
