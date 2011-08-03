package com.criticcomrade.etl;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import com.criticcomrade.etl.query.*;
import com.criticcomrade.etl.query.db.*;
import com.google.gson.JsonSyntaxException;

public class Main extends Thread {
    
    private static final String CMD_PRINT_OPTIONS = "-?";
    private static final String CMD_FROM_QUEUE = "--from-queue";
    private static final String CMD_CURRENT_LISTS = "--from-current-lists";
    private static final String CMD_MOVE_RUNS = "--move-run";
    private static final List<String> CMD_LIST = Arrays.asList(CMD_CURRENT_LISTS, CMD_FROM_QUEUE, CMD_MOVE_RUNS, CMD_PRINT_OPTIONS);
    
    private static final String PARAM_NUM_THREADS = "--threads";
    private static final String PARAM_MAX_RUNTIME = "--max-runtime";
    private static final List<String> PARAM_LIST = Arrays.asList(PARAM_MAX_RUNTIME, PARAM_NUM_THREADS);
    
    private static Connection conn;
    
    public static void main(String[] args) throws JsonSyntaxException, IOException, SQLException, InterruptedException, ParameterException,
	    AmbiguousQueryException {
	
	List<String> argList = new ArrayList<String>();
	Collections.addAll(argList, args);
	
	conn = DaoUtility.getConnection();
	try {
	    
	    Set<Thread> threads = new HashSet<Thread>();
	    
	    do {
		
		Map<String, String> params;
		try {
		    params = buildOneCommandParameterMap(argList);
		} catch (ParameterException e1) {
		    printOptions(e1.toString());
		    throw e1;
		}
		
		int numThreads = parseParameterInteger(params, PARAM_NUM_THREADS, 1);
		int maxRuntime = parseParameterInteger(params, PARAM_MAX_RUNTIME, 60 * 24);
		
		if (params.keySet().contains(CMD_CURRENT_LISTS)) {
		    
		    threads.add(new RottenTomatoesListsToQueue(conn));
		    
		} else if (params.keySet().contains(CMD_FROM_QUEUE)) {
		    for (int i = 0; i < numThreads; i++) {
			threads.add(new RottenTomatoesFromQueueEtl(conn, maxRuntime));
		    }
		} else if (params.keySet().contains(CMD_MOVE_RUNS)) {
		    new RtControllerDao(conn).nextRun();
		} else if (params.keySet().contains(CMD_PRINT_OPTIONS)) {
		    printOptions(null);
		    return;
		} else {
		    printOptions("Invalid option");
		    System.exit(1);
		}
		
		System.out.println(params.toString());
		
	    } while (argList.size() > 0);
	    
	    for (Thread thread : threads) {
		thread.start();
	    }
	    
	    for (Thread thread : threads) {
		thread.join();
	    }
	    
	} finally {
	    conn.close();
	}
	
    }
    
    private static int parseParameterInteger(Map<String, String> params, String paramName, int defaultValue) {
	int ret;
	if (params.containsKey(paramName)) {
	    try {
		ret = Integer.parseInt(params.get(paramName));
	    } catch (NumberFormatException e) {
		printOptions("Unable to parse parameter value: " + paramName + " " + params.get(paramName));
		throw e;
	    }
	} else {
	    ret = defaultValue;
	}
	return ret;
    }
    
    private static Map<String, String> buildOneCommandParameterMap(List<String> args) throws ParameterException {
	
	Map<String, String> ret = new HashMap<String, String>();
	
	if (args.size() < 1) {
	    throw new ParameterException("Too few parameters");
	}
	
	if (CMD_LIST.contains(args.get(0))) {
	    ret.put(args.remove(0), null);
	}
	
	while ((args.size() > 0) && !CMD_LIST.contains(args.get(0))) {
	    String arg = args.remove(0);
	    
	    if (PARAM_LIST.contains(arg)) { // Parameters expecting 1 argument
		if (args.size() < 1) {
		    throw new ParameterException("Parameter " + arg + " expects an argument");
		}
		ret.put(arg, args.remove(0));
	    } else { // Unexpected parameter
		throw new ParameterException("Unexpected parameter " + arg);
	    }
	}
	
	return ret;
	
    }
    
    private static void printOptions(String msg) {
	if (msg != null) {
	    System.err.println("Error: " + msg);
	}
	System.err.println("Usage: <command> [" + PARAM_NUM_THREADS + " <#>] [" + PARAM_MAX_RUNTIME + " <#>] [<command>...]");
	System.err.println("\t" + CMD_PRINT_OPTIONS + "\t\t\t\tPrint these options and quit.");
	System.err.println("\t" + CMD_CURRENT_LISTS + "\t\tEnsure the current box office, in theaters, opening, and upcoming movies " +
	        "from RottenTomatoes are on the queue and active.");
	System.err.println("\t" + CMD_FROM_QUEUE + "\tETL movies from the queue.");
	System.err
	        .println("\t" + CMD_MOVE_RUNS + "\tChange what the current run is considered to be (before running other commands). " +
	                "Will make all current runs stop after their current movie.");
    }
    
}