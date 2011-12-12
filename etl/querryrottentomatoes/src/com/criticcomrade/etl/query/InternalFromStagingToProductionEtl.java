package com.criticcomrade.etl.query;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.criticcomrade.etl.EtlThread;
import com.criticcomrade.etl.data.DataItem;
import com.criticcomrade.etl.query.db.AmbiguousQueryException;
import com.criticcomrade.etl.query.db.DaoUtility;
import com.criticcomrade.etl.query.db.DataItemDao;
import com.criticcomrade.etl.query.db.ProductionDao;
import com.criticcomrade.etl.query.db.RtQueueDao;

public class InternalFromStagingToProductionEtl extends EtlThread {

    private Iterator<String> freshMoviesIter = null;
    private Iterator<String> newMoviesIter = null;

    public InternalFromStagingToProductionEtl(Connection conn, int maxRuntime) throws AmbiguousQueryException {
        super(conn, maxRuntime);
    }

    @Override
    protected boolean haveReasonToQuit(List<String> reasons) {
        return false;
    }

    /**
     * Will return the id of the next movie on the queue which should be ETL'd. This movie will not be locked (at the
     * time of this call). These will first be movies which have been pushed and have changed since then, and then
     * movies which have not yet been pushed.
     * 
     * @return A RottenTomatoes movie ID to ETL
     */
    @Override
    protected String getNextIdToEtl() {

        final DataItemDao dataItemDao = new DataItemDao(conn);

        // ///////////////

        if (freshMoviesIter == null) {

            List<String> freshMovies = dataItemDao.getPushedMoviesWithChanges();
            freshMoviesIter = freshMovies.iterator();

        }

        if (freshMoviesIter.hasNext()) {
            return freshMoviesIter.next();
        }

        // ///////////////

        if (newMoviesIter == null) {

            List<String> newMovies = dataItemDao.getNonPushedMovies();
            newMoviesIter = newMovies.iterator();

        }

        if (newMoviesIter.hasNext()) {
            return newMoviesIter.next();
        }

        // ///////////////

        return null;

    }

    @Override
    public DataItem doEtlImpl(String id) {

        RtQueueDao rtQueueDao = new RtQueueDao(conn);
        final DataItemDao dataItemDao = new DataItemDao(conn);
        final ProductionDao productionDao = new ProductionDao(conn);

        long startTime = System.currentTimeMillis();
        Date nowDate = new Date();
        String result;

        if (!RtQueueDao.getMovieLock(id, rtQueueDao)) {
            result = "Locked";
        } else {

            try {

                DataItem movie = dataItemDao.findItemById(id);

                boolean changed = dataItemDao.getHasChangedSincePush(movie);
                if (changed) {
                    List<Integer> itemList = productionDao.pushMovie(movie);
                    dataItemDao.updatePushDate(itemList, nowDate);
                    result = "Updated";
                } else {
                    result = "No change";
                }

            } catch (Exception e) {
                e.printStackTrace();
                result = e.toString();
            } finally {
                rtQueueDao.removeMovieLock(id, rtQueueDao);
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("%s %s %s", new SimpleDateFormat().format(new Date(endTime)), id, result));

        return null;

    }

    public static void main(String args[]) throws SQLException, IOException, AmbiguousQueryException {

        InternalFromStagingToProductionEtl o = new InternalFromStagingToProductionEtl(DaoUtility.getConnection(),
                1000 * 60 * 10);
        EtlThread.printAttrsTree("", o.doEtlImpl("770687943"));

    }
}
