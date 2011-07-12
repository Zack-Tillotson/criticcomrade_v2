package com.criticcomrade.etl;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import com.criticcomrade.etl.query.*;
import com.criticcomrade.etl.query.db.DaoUtility;
import com.criticcomrade.etl.scrape.RottenTomatoesReviewsScrape;
import com.google.gson.JsonSyntaxException;

public class Main extends Thread {
    
    private static final String PRINT_OPTIONS = "-?";
    private static final String FROM_QUEUE = "--from-queue";
    private static final String CURRENT_LISTS = "--from-current-lists";
    private static final String SCRAPE_REVIEWS = "--scrape-reviews";
    
    private static Connection conn;
    
    public static void main(String[] args) throws JsonSyntaxException, IOException, SQLException, InterruptedException {
	
	if (args.length < 1) {
	    printOptions();
	    System.exit(1);
	}
	
	conn = DaoUtility.getConnection();
	
	if (args[0].equals(CURRENT_LISTS)) {
	    
	    new RottenTomatoesListsToQueue(conn).start();
	    
	} else if (args[0].equals(FROM_QUEUE)) {
	    
	    if (args.length != 2) {
		printOptions();
		System.exit(1);
	    }
	    
	    int numThreads;
	    try {
		numThreads = Integer.parseInt(args[1]);
	    } catch (NumberFormatException e) {
		printOptions();
		throw e;
	    }
	    
	    List<Thread> threads = new ArrayList<Thread>();
	    for (int i = 0; i < numThreads; i++) {
		threads.add(new RottenTomatoesFromQueueEtl(conn));
	    }
	    
	    for (Thread thread : threads) {
		thread.start();
	    }
	    
	    // CONSIDER Add some logic to be able to control the ETL'ing threads gracefully
	    
	} else if (args[0].equals(SCRAPE_REVIEWS)) {
	    
	    if (args.length != 2) {
		printOptions();
		System.exit(1);
	    }
	    
	    int numThreads;
	    try {
		numThreads = Integer.parseInt(args[1]);
	    } catch (NumberFormatException e) {
		printOptions();
		throw e;
	    }
	    
	    List<Thread> threads = new ArrayList<Thread>();
	    for (int i = 0; i < numThreads; i++) {
		threads.add(new RottenTomatoesReviewsScrape(conn));
	    }
	    
	    for (Thread thread : threads) {
		thread.start();
	    }
	    
	    // CONSIDER Add some logic to be able to control the ETL'ing threads gracefully
	    
	} else {
	    printOptions();
	    System.exit(1);
	}
	
    }
    
    private static void printOptions() {
	System.err.println("Usage: options");
	System.err.println("\t" + PRINT_OPTIONS + "\t\t\t\tPrint these options");
	System.err.println("\t" + CURRENT_LISTS + "\t\tEnsure the current box office, in theaters, opening, and upcoming movies " +
	        "from RottenTomatoes are on the queue");
	System.err.println("\t" + FROM_QUEUE + " <# Threads>\tETL movies from the queue.");
    }
    
}