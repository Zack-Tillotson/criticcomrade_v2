package com.criticcomrade.etl.query;

import java.io.IOException;

import com.criticcomrade.etl.query.data.*;
import com.google.gson.JsonSyntaxException;

public class Main {
    
    public static void main(String[] args) {
	
	try {
	    RottenTomatoesMovieQuery mq = new RottenTomatoesMovieQuery("10", "http://rottentomatoes.com/whatever/");
	    printAttrsTree("", mq);
	} catch (JsonSyntaxException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	
    }
    
    private static void printAttrsTree(String tab, DataItem item) {
	for (Attribute attr : item.getAttributes()) {
	    System.out.println(String.format("%s%s = %s", tab, attr.attribute, attr.value));
	}
	System.out.println();
	for (DataItem dataItem : item.getSubItems()) {
	    printAttrsTree(tab + "\t", dataItem);
	}
    }
}
