package com.pingcap.poc.dbbench;

public class Config {
    String host;
    int port;
    String database;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getCol_num() {
        return col_num;
    }

    public void setCol_num(int col_num) {
        this.col_num = col_num;
    }

    public int getRow_num() {
        return row_num;
    }

    public void setRow_num(int row_num) {
        this.row_num = row_num;
    }

    String user;
    String password;
    int col_num;
    int row_num;
    int thread_num;
    int field_len;
    int batch_num;

    public int getBatch_num() {
        return batch_num;
    }

    public void setBatch_num(int batch_num) {
        this.batch_num = batch_num;
    }

    public int getField_len() {
        return field_len;
    }

    public void setField_len(int field_len) {
        this.field_len = field_len;
    }

    public int getThread_num() {
        return thread_num;
    }

    public void setThread_num(int thread_num) {
        this.thread_num = thread_num;
    }
}
