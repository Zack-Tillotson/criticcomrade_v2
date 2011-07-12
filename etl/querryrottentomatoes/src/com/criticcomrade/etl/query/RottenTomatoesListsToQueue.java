package com.criticcomrade.etl.query;

import java.io.IOException;
import java.sql.Connection;

import com.criticcomrade.api.data.MovieShort;
import com.criticcomrade.api.main.RottenTomatoesApi;
import com.criticcomrade.etl.query.db.RtQueueDao;
import com.google.gson.JsonSyntaxException;

public class RottenTomatoesListsToQueue extends Thread {
    
    private final Connection conn;
    
    public RottenTomatoesListsToQueue(Connection conn) {
	this.conn = conn;
    }
    
    @Override
    public void run() {
	try {
	    
	    for (MovieShort ms : RottenTomatoesApi.getBoxOfficeMovies()) {
		addMovieToQueue(ms.id);
	    }
	    
	    for (MovieShort ms : RottenTomatoesApi.getInTheatersMovies()) {
		addMovieToQueue(ms.id);
	    }
	    
	    for (MovieShort ms : RottenTomatoesApi.getOpeningMovies()) {
		addMovieToQueue(ms.id);
	    }
	    
	    for (MovieShort ms : RottenTomatoesApi.getUpcomingMovies()) {
		addMovieToQueue(ms.id);
	    }
	    
	} catch (JsonSyntaxException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
    
    // Just add it to the queue to be picked up later
    private void addMovieToQueue(String id) {
	RtQueueDao dao = new RtQueueDao(conn);
	if (RtQueueDao.getMovieLock(id, dao)) {
	    System.out.println(String.format("RT.com Id: %s", id));
	    dao.removeMovieLock(id, dao);
	}
    }
    
}
