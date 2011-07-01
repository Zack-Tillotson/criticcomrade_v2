package com.criticcomrade.etl.query;

import java.io.IOException;
import java.util.Date;

import com.criticcomrade.api.data.MovieShort;
import com.criticcomrade.api.main.RottenTomatoesApi;
import com.criticcomrade.etl.query.data.*;
import com.google.gson.JsonSyntaxException;

public class Main {
    
    public static void main(String[] args) throws JsonSyntaxException, IOException {
	
	for (MovieShort ms : RottenTomatoesApi.getBoxOfficeMovies()) {
	    etlMovie(ms, 1000 * 60 * 60 * 24);
	}
    }
    
    public static void etlMovie(MovieShort ms, long staleTimePeriod) throws IOException {
	
	if (((new Date()).getTime() - RtQueueDao.getLastQueryDate(ms.id).getTime()) > staleTimePeriod) {
	    
	    String result;
	    try {
		RottenTomatoesMovieQuery mq = new RottenTomatoesMovieQuery(ms.id, ms.links.self);
		DataItemDao.putDataItem(mq);
		printAttrsTree("", mq);
		result = "success";
	    } catch (JsonSyntaxException e) {
		e.printStackTrace();
		result = e.toString();
	    } catch (IOException e) {
		e.printStackTrace();
		result = e.toString();
	    }
	    System.out.println("=== Result: " + result);
	    
	}
	
    }
    
    private static void printAttrsTree(String tab, DataItem item) {
	System.out.println(tab + item.getType() + " " + item.getId());
	for (Attribute attr : item.getAttributes()) {
	    System.out.println(String.format("%s%s = %s", tab, attr.attribute, attr.value));
	}
	System.out.println();
	for (DataItem dataItem : item.getSubItems()) {
	    printAttrsTree(tab + "\t", dataItem);
	}
    }
}