package com.pingcap.poc.dbbench;

import java.util.ArrayList;

public class Monitor implements Runnable {
    ArrayList<DBBench> lst;
    int interval;

    public Monitor(ArrayList<DBBench> lst, int interval){
        this.lst = lst;
        this.interval = interval;
    }

    @Override
    public void run() {
        long l_s = System.currentTimeMillis();
        long l1 = l_s;
        long rows1 = 0;
        while(true){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long l2 = System.currentTimeMillis();
            long dur = l2 - l1;
            long dur1 = l2 - l_s;

            if ( dur / 1000.0 > interval){
                // statistics
                l1 = l2;
                long rows = 0;
                for (DBBench dbBench : lst)
                    rows += dbBench.getRows_num();

                System.out.printf("%d s: current qps is %d, total qps is %d, total %d rows inserted.\n",
                        (int)(dur1 / 1000.0 / interval * interval),
                        (rows - rows1) * 1000 / dur ,
                        rows * 1000 / dur1,
                        rows);

                rows1 = rows;

                boolean done = true;
                for (DBBench dbBench : lst)
                    if (!dbBench.getDone()) {
                        done = false;
                        break;
                    }

                if (done)
                    return;
            }
        }
    }
}
