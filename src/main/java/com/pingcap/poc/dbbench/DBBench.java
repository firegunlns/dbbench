package com.pingcap.poc.dbbench;

import org.apache.commons.cli.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DBBench {
    Connection conn = null;

    Config config = new Config();

    public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException, ParseException {
        long num = 10000000;
        int thread_num = 100;

        Config config = new Config();

        CommandLineParser parser = new BasicParser();
        Options options = new Options();
        options.addOption("?","help",false,"Print this usage information");
        options.addOption("h","host",true,"mysql/tidb server name/ip");
        options.addOption("p","password",true,"mysql/tidb user password");
        options.addOption("u","user",true,"mysql/tidb server user name");
        options.addOption("P","port",true,"mysql/tidb server port");
        options.addOption("d","database",true,"mysql/tidb server database");
        options.addOption("t","threads",true,"thread num");
        options.addOption("n","records num",true,"records num to insert");
        options.addOption("f","fields num",true,"fields num of table to test");

        //Parse the program arguments
        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption('?')){
            System.out.println("java -jar dbbench.jar [-hPpud]");
            System.exit(0);
        }

        config.setHost(commandLine.getOptionValue('h', "127.0.0.1"));
        config.setUser(commandLine.getOptionValue('u', "root"));
        config.setPassword(commandLine.getOptionValue('p', ""));
        config.setPort(Integer.parseInt(commandLine.getOptionValue('P', "4000")));
        config.setDatabase(commandLine.getOptionValue('d', "test"));
        config.setCol_num(Integer.parseInt(commandLine.getOptionValue('f', "20")));
        config.setThread_num(Integer.parseInt(commandLine.getOptionValue('t', "1")));
        config.setRow_num(Integer.parseInt(commandLine.getOptionValue('n', "1000")));

        DBBench inst = new DBBench();
        inst.prepare();
        inst.close();

        long l1 = System.currentTimeMillis();
        ExecutorService svc = Executors.newFixedThreadPool(thread_num );
        for (int i = 0; i < thread_num; i++ ){
            svc.execute(new Runnable() {
                @Override
                public void run() {
                    DBBench inst1 = new DBBench();
                    try {
                        inst1.runInsert(1, config.getRow_num() / config.getThread_num());
                    }catch (Exception e){
                    }
                    inst1.close();
                    System.out.printf("thread %d finished!\n", Thread.currentThread().getId());
                }
            });
        }

        svc.shutdown();
        svc.awaitTermination(1, TimeUnit.HOURS);
        long l2 = System.currentTimeMillis();
        long dur = l2 - l1;
        System.out.printf("time used is %d ms, speed is %d rows /s\n", dur, (int)(num * 1000.0 / dur));
    }

    Connection open(String host, String user, String password, int port,String database) throws SQLException, ClassNotFoundException {
        if (conn == null) {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String url = String.format("jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true", host, port, database);
            conn = DriverManager.getConnection(url, user, password);
        }
        return conn;
    }

    void close(){
        if (conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Connection getConn() throws SQLException, ClassNotFoundException {
        return open(config.getHost(), config.getUser(), config.getPassword(), config.getPort(), config.getDatabase());
    }

    public boolean prepare() throws SQLException, ClassNotFoundException {
        Connection conn = getConn();
        String sql = "drop table if exists test1";
        conn.prepareCall(sql).execute();

        StringBuffer sql1 = new StringBuffer();
        sql1.append("create table test1(id int auto_increment primary key");
        for (int i = 0; i < config.getCol_num() - 1; i ++){
            String fld = String.format("fld%d varchar(%d)", i, config.getField_len());
            sql1.append(", " + fld);
        }
        sql1.append(")");

        conn.prepareStatement(sql1.toString()).execute();

        return true;
    }

    public boolean runInsert(int thread, long num /* in seconds */) throws SQLException, ClassNotFoundException {
        Connection conn = getConn();
        StringBuffer sb = new StringBuffer();
        StringBuffer sb1 = new StringBuffer();
        sb.append("insert into test1");

        for (int i = 0; i < config.col_num - 1; i++ ){
            String fld = String.format("fld%d", i);
            if (i == 0) {
                sb.append("(" + fld);
                sb1.append("(?");
            }
            else {
                sb.append(", " + fld);
                sb1.append(",?");
            }
        }

        sb.append(") values ");
        sb.append(sb1.toString());
        sb.append(")");

        PreparedStatement ps = conn.prepareStatement(sb.toString());
        Random r = new Random(System.currentTimeMillis());
        byte[] data = new byte[config.getField_len()];
        int cnt = 0;
        while(cnt < num){
            for (int i = 0; i < config.col_num - 1; i++ ){
                r.nextBytes(data);
                ps.setString(i + 1, new String(data));
            }
            ps.addBatch();
            cnt ++;

            if (cnt % config.getBatch_num() == 0) {
                ps.executeBatch();
                System.out.printf("(thread:%d) %d rows inserted.\n", Thread.currentThread().getId(), batch_num);
            }
        }

        if (cnt % config.getBatch_num() != 0){
            ps.executeBatch();
        }

        ps.close();

        return true;
    }
}
