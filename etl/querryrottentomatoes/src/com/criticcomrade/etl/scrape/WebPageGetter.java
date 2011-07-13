package com.criticcomrade.etl.scrape;

import java.io.*;
import java.net.*;
import java.util.Iterator;

public class WebPageGetter implements Iterator<String> {
    
    private final DataInputStream dis;
    private String line;
    
    public WebPageGetter(String url) throws IOException {
	dis = getDataInputStream(url);
    }
    
    private DataInputStream getDataInputStream(String urlString) throws IOException {
	
	URL url;
	InputStream is = null;
	DataInputStream dis;
	try {
	    url = new URL(urlString);
	    is = url.openStream(); // throws an IOException
	    dis = new DataInputStream(new BufferedInputStream(is));
	    
	    line = dis.readLine();
	    
	    return dis;
	    
	} catch (MalformedURLException mue) {
	    mue.printStackTrace();
	    throw mue;
	} catch (IOException ioe) {
	    ioe.printStackTrace();
	    throw ioe;
	}
	
    }
    
    public void closeDataInputStream() {
	try {
	    dis.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
    
    @Override
    public boolean hasNext() {
	return line != null;
    }
    
    @Override
    public String next() {
	String ret = line;
	try {
	    line = dis.readLine();
	} catch (IOException e) {
	    e.printStackTrace();
	    line = null;
	}
	return ret;
    }
    
    @Override
    public void remove() {
	
    }
}
