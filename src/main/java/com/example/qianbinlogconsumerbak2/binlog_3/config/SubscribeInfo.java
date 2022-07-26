package com.example.qianbinlogconsumerbak2.binlog_3.config;

import java.util.List;

public class SubscribeInfo {
    // @leimo database为唯一索引，标明订阅该database的tables
    private String database;
    private List<String> tables;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }
}
