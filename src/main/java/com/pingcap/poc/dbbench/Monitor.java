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
        while(true){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long l2 = System.currentTimeMillis();
            long dur = l2 - l1;
            long dur1 = l2 - l_s;
            l1 = l2;
            if ( dur / 1000.0 > interval){
                // statistics
                long rows = 0;
                for (DBBench dbBench : lst)
                    rows += dbBench.getRows_num();

                System.out.printf("%d s: qps is %d, total %d rows inserted.\n", dur1 / interval * interval,
                        rows * 1000 / dur1, rows);

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
