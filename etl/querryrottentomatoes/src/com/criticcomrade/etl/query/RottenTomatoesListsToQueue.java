package com.criticcomrade.etl.query;

import java.io.IOException;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;

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

    /**
     * Add it to the queue and give it a nice last found date to be picked up later
     * 
     * @param id
     */
    private void addMovieToQueue(String id) {
        RtQueueDao dao = new RtQueueDao(conn);
        if (RtQueueDao.getMovieLock(id, dao)) {
            System.out.println(String.format("%s RT.com Id: %s", new SimpleDateFormat().format(new Date()), id));
            dao.setAsActive(id);
            dao.removeMovieLock(id, dao);
        }
    }

}
