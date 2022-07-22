/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.qianbinlogconsumerbak2.mysql.position;


import com.example.qianbinlogconsumerbak2.mysql.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BinlogPositionManager {
    private Logger logger = LoggerFactory.getLogger(BinlogPositionManager.class);

    private DataSource dataSource;
    private Config config;

    private String binlogFilename;
    private Long nextPosition;

    public BinlogPositionManager(Config config, DataSource dataSource) {
        this.config = config;
        this.dataSource = dataSource;
    }

    public void initBeginPosition() throws Exception {
        initPositionDefault();

        if (binlogFilename == null || nextPosition == null) {
            throw new Exception("binlogFilename | nextPosition is null.");
        }
    }

    private void initPositionDefault() throws Exception {

        try {
            initPositionFromDataSource();
        } catch (Exception e) {
            logger.error("Init position from mq error.", e);
        }

        if (binlogFilename == null || nextPosition == null) {
            initPositionFromBinlogTail();
        }

    }

    private void initPositionFromDataSource() throws SQLException {

        // @leimo 1. create database binlog and table position
        // 仅在 binlog.position表没有创建时初始化 database & table
        createPositionDatabaseAndTable();

        // @leimo 2. 查询 position数据表，填充fileName 和 position
        doInitPositionFromDataSource();
    }

    private void createPositionDatabaseAndTable() throws SQLException {
        String CREATE_DATABASE = "create database if not exists binlog;";

        String CREATE_TABLE = "create table if not exists binlog.position\n" +
                "(\n" +
                "    serverId BIGINT         not null,\n" +
                "    `group`  varchar(200) not null,\n" +
                "    fileName varchar(200) not null,\n" +
                "    position BIGINT         not null\n" +
                ");\n";
// @leimo index不能重复创建
//        String CREATE_GROUP_INDEX = "create unique index position_group_uindex\n" +
//                "    on binlog.position (`group`);\n";
//
//        String CREATE_SERVERID_INDEX = "create unique index position_serverId_uindex\n" +
//                "    on binlog.position (serverId);";

//        String INSERT = "INSERT INTO binlog.position set serverId=123,`group`='dd',fileName='file1',position=1000;";
        Connection connection = null;

        try {
            connection = dataSource.getConnection();
            int createDatabaseRes = connection.createStatement().executeUpdate(CREATE_DATABASE);
            int createTableRes = connection.createStatement().executeUpdate(CREATE_TABLE);
//            int createGroupIndex = connection.createStatement().executeUpdate(CREATE_GROUP_INDEX);
//            int createServerIdIndex = connection.createStatement().executeUpdate(CREATE_SERVERID_INDEX);
//            try {
//                int res = connection.createStatement().executeUpdate(INSERT);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
            logger.info("[BinlogPositionManager]: create position database and table");
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void doInitPositionFromDataSource() throws SQLException {
        String QUERY_POSITION = "select fileName,position from binlog.position where `group`=" + "'" + config.group + "'";
//        String QUERY_POSITION = "select fileName,position from binlog.position where `group`='dd'" ;
        Connection conn = null;
        ResultSet queryPositionRes = null;
        try {
            Connection connection = dataSource.getConnection();
            // query position
            queryPositionRes = connection.createStatement().executeQuery(QUERY_POSITION);
            while (queryPositionRes.next()) {
                binlogFilename = queryPositionRes.getString("fileName");
                nextPosition = queryPositionRes.getLong("position");
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (queryPositionRes != null) {
                queryPositionRes.close();
            }
        }
        logger.info("[BinlogPositionManager]: init from datasource, fileName is {}, position is {}",binlogFilename,nextPosition);
    }

    private void initPositionFromBinlogTail() throws SQLException {
        String sql = "SHOW MASTER STATUS";

        Connection conn = null;
        ResultSet rs = null;

        try {
            Connection connection = dataSource.getConnection();
            rs = connection.createStatement().executeQuery(sql);

            while (rs.next()) {
                binlogFilename = rs.getString("File");
                nextPosition = rs.getLong("Position");
            }

        } finally {

            if (conn != null) {
                conn.close();
            }
            if (rs != null) {
                rs.close();
            }
        }
        logger.info("[BinlogPositionManager]: init from binlogTail, fileName is {}, position is {}",binlogFilename,nextPosition);
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public Long getPosition() {
        return nextPosition;
    }
}
