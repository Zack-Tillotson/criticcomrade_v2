package com.criticcomrade.etl.scrape;

import java.util.List;
import java.util.regex.*;

public class NumberOfPagesParser extends StringListParser<Integer> {
    
    private enum Status {
	BEFORE_CONTENT, BEFORE_PAGE
    }
    
    public NumberOfPagesParser(List<String> lines) {
	super(lines);
    }
    
    @Override
    protected Integer parseImpl() {
	
	Pattern p;
	Pattern p2 = Pattern.compile("No Critic Reviews for ");
	Matcher m;
	Matcher m2;
	
	Status status = Status.BEFORE_CONTENT;
	
	while (isMoreLinesExist()) {
	    
	    String line = getNextLine();
	    
	    switch (status) {
		case BEFORE_CONTENT:

		    p = Pattern.compile("<div\\s+class=\"content\\s*\"\\s+");
		    m = p.matcher(line);
		    if (m.find()) {
			status = Status.BEFORE_PAGE;
		    } else {
			m2 = p2.matcher(line); // No reviews for this movie
			if (m2.find()) {
			    return 0;
			}
		    }
		    
		    break;
		case BEFORE_PAGE:

		    p = Pattern.compile(">Page\\s+([^\\s]+)\\s+of\\s+([^\\s<]+)");
		    m = p.matcher(line);
		    if (m.find()) {
			return Integer.parseInt(m.group(2));
		    }
		    
		    break;
	    }
	    
	}
	
	return null;
    }
}
