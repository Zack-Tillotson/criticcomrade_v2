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
	Matcher m;
	
	Status status = Status.BEFORE_CONTENT;
	
	while (isMoreLinesExist()) {
	    
	    String line = getNextLine();
	    
	    switch (status) {
		case BEFORE_CONTENT:

		    p = Pattern.compile("<div\\s+class=\"content\\s*\"\\s+");
		    m = p.matcher(line);
		    if (m.find()) {
			status = Status.BEFORE_PAGE;
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
