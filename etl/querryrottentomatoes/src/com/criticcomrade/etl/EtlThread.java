package com.criticcomrade.etl;

import java.io.IOException;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.criticcomrade.etl.data.Attribute;
import com.criticcomrade.etl.data.DataItem;
import com.criticcomrade.etl.query.db.AmbiguousQueryException;
import com.criticcomrade.etl.query.db.RtControllerDao;

public abstract class EtlThread extends Thread {

    protected static final int ACTIVE_TIME_PERIOD_START = 1000 * 60 * 60 * 24 * 7; // 7 Day
    protected static final int ACTIVE_TIME_PERIOD_END = 1000 * 60 * 60 * 24; // 1 Day
    protected static final int STALE_TIME_PERIOD = 1000 * 60 * 60 * 24 * 30; // 1 Month
    protected static final int API_THROTTLE_PERIOD = 1000 * 60 * 60 * 24; // 1 Day
    protected static final int API_THROTTLE_AMOUNT = 9500; // Don't do more than 9000 calls a day

    protected final Connection conn;
    protected final Date startWhen;
    protected final int maxRuntimeMins;

    private int currentRunName;

    public EtlThread(Connection conn, int maxRuntime) throws AmbiguousQueryException {
        this.conn = conn;
        startWhen = new Date();
        maxRuntimeMins = maxRuntime * 1000 * 60;
    }

    @Override
    public void run() {

        try {
            try {
                currentRunName = new RtControllerDao(conn).getCurrentRunName();
            } catch (AmbiguousQueryException e) {
                System.out.println("Weird run name error");
            }

            System.out.println(String.format("%s Starting ETL [Current Run: %s, Running Until: %s]",
                    new SimpleDateFormat().format(new Date()), currentRunName, new SimpleDateFormat().format(new Date()
                            .getTime()
                            + maxRuntimeMins)));

            List<String> reasonsToQuit = new ArrayList<String>();
            String id = null;
            while (shouldContinueToEtl(reasonsToQuit)) {
                if ((id = getNextIdToEtl()) != null) {
                    doEtlImpl(id);
                } else {
                    System.out.println("No movies to ETL found. Exiting...");
                    return;
                }
            }

            System.out.println("Failed conditions to continue ETL. " + reasonsToQuit.toString() + " Exiting...");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Will quit after the set throttle period,
     * 
     * @return
     * @throws AmbiguousQueryException
     */
    private boolean shouldContinueToEtl(List<String> reasons) {

        final Date nowWhen = new Date();

        boolean bossSaysStop;
        try {
            bossSaysStop = (new RtControllerDao(conn).getCurrentRunName()) != currentRunName;
        } catch (AmbiguousQueryException e) {
            bossSaysStop = false;
        }
        if (bossSaysStop) {
            reasons.add("Boss said stop");
        }

        boolean haveRunTooLong = (nowWhen.getTime() - startWhen.getTime()) >= maxRuntimeMins;
        if (haveRunTooLong) {
            reasons.add("Past maximum runtime");
        }

        return !bossSaysStop && !haveRunTooLong && !haveReasonToQuit(reasons);
    }

    protected abstract boolean haveReasonToQuit(List<String> reasons);

    /**
     * Will find the next id to etl
     * 
     * @return
     */
    protected abstract String getNextIdToEtl();

    /**
     * Do the actual ETL and return the object
     * 
     * @param id
     * @return
     */
    public abstract DataItem doEtlImpl(String id) throws IOException;

    /**
     * Print the data item in nested tree form to standard out
     * 
     * @param tab
     * @param item
     */
    public static void printAttrsTree(String tab, DataItem item) {
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