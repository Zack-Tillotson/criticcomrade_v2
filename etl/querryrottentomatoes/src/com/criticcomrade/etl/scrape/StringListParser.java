package com.criticcomrade.etl.scrape;

import java.util.List;

public abstract class StringListParser<T> {
    
    private boolean lastParseSuccessful = false;
    private T lastResult;
    private final List<String> lines;
    
    public StringListParser(List<String> lines) {
	this.lines = lines;
    }
    
    /**
     * Parses the next object, returns true if it was sucessfully found. This can be run repeatedly
     * to get successive items in turn.
     * 
     * @return
     */
    public final boolean parse() {
	lastParseSuccessful = (lastResult = parseImpl()) != null;
	return lastParseSuccessful;
    }
    
    protected abstract T parseImpl();
    
    public final boolean getLastParsesucceeded() {
	return lastParseSuccessful;
    }
    
    public final T getObject() throws ResultDoesNotExistException {
	if (!getLastParsesucceeded()) {
	    throw new ResultDoesNotExistException();
	}
	return lastResult;
    }
    
    protected final String getNextLine() {
	return lines.remove(0);
    }
    
    protected final boolean isMoreLinesExist() {
	return lines.size() > 0;
    }
    
}
