package com.pingcap.poc.dbbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

public class DBBench implements Runnable{
    private Connection conn = null;
    private final Config config;
    private long rows_num = 0;
    private boolean done = false;

    public DBBench(Config config){
        this.config = config;
    }

    public void setRows_num(long rows_num) {
        this.rows_num = rows_num;
    }

    public long getRows_num(){
        return this.rows_num;
    }

    public boolean getDone(){
        return done;
    }

    public void setDone(boolean done){
        this.done = done;
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

    public void prepare() throws SQLException, ClassNotFoundException {
        Connection conn = getConn();
        String sql = "drop table if exists test1";
        conn.prepareCall(sql).execute();

        StringBuilder sql1 = new StringBuilder();
        sql1.append("create table test1(id int auto_increment primary key");
        for (int i = 0; i < config.getCol_num() - 1; i ++){
            String fld = String.format("fld%d varchar(%d)", i, config.getField_len());
            sql1.append(", ").append(fld);
        }
        sql1.append(")");

        conn.prepareStatement(sql1.toString()).execute();
    }

    public void runInsert(long num /* in seconds */) throws SQLException, ClassNotFoundException {
        Connection conn = getConn();
        StringBuilder sb = new StringBuilder();
        StringBuilder sb1 = new StringBuilder();
        sb.append("insert into test1");

        for (int i = 0; i < config.getCol_num() - 1; i++ ){
            String fld = String.format("fld%d", i);
            if (i == 0) {
                sb.append("(").append(fld);
                sb1.append("(?");
            }
            else {
                sb.append(", ").append(fld);
                sb1.append(",?");
            }
        }

        sb.append(") values ");
        sb.append(sb1);
        sb.append(")");

        PreparedStatement ps = conn.prepareStatement(sb.toString());
        Random r = new Random(System.currentTimeMillis());
        byte[] data = new byte[config.getField_len()];
        int cnt = 0;
        while(cnt < num){
            for (int i = 0; i < config.getCol_num() - 1; i++ ){
                r.nextBytes(data);
                ps.setString(i + 1, new String(data));
            }
            ps.addBatch();
            cnt ++;

            if (cnt % config.getBatch_num() == 0) {
                ps.executeBatch();
                System.out.printf("(thread:%d) %d rows inserted.\n", Thread.currentThread().getId(), config.getBatch_num());
                setRows_num(cnt);
            }
        }

        if (cnt % config.getBatch_num() != 0){
            ps.executeBatch();
            setRows_num(cnt);
        }

        ps.close();
    }

    @Override
    public void run() {
        try {
            runInsert(config.getRow_num() / config.getThread_num());
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        close();
        setDone(true);

        System.out.printf("thread %d finished!\n", Thread.currentThread().getId());
    }
}
