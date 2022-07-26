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

package com.example.qianbinlogconsumerbak2.binlog_2.schema;

import com.example.qianbinlogconsumerbak2.binlog_2.binlog.EventProcessor;

import com.example.qianbinlogconsumerbak2.binlog_2.config.Config;
import com.example.qianbinlogconsumerbak2.binlog_2.config.SubscribeInfo;
import com.example.qianbinlogconsumerbak2.binlog_2.schema.column.ColumnParser;
import com.example.qianbinlogconsumerbak2.binlog_2.util.SqlUtils;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Database {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);

    private static final String TABLE_QUERY = "select table_name,column_name,data_type,column_type,character_set_name " +
        "from information_schema.columns " +
        "where table_schema = ?";

    private Config config;

    private static List<String> TARGET_TABLES = Lists.newArrayList();

    private String name;

    private DataSource dataSource;

    private Map<String, Table> tableMap = new HashMap<String, Table>();

    public Database(String name, DataSource dataSource, Config config) {
        this.name = name;
        this.dataSource = dataSource;
        this.config = config;
        for (SubscribeInfo info : config.subscribeInfos) {
            TARGET_TABLES.addAll(info.getTables());
        }
    }

    public void init() throws SQLException {
        SqlUtils.executeQuery(dataSource,TABLE_QUERY,(rs)->{
            try {
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    String colName = rs.getString(2);
                    String dataType = rs.getString(3);
                    String colType = rs.getString(4);
                    String charset = rs.getString(5);

                    if (!TARGET_TABLES.contains(tableName)) {
                        continue;
                    }

                    ColumnParser columnParser = ColumnParser.getColumnParser(dataType, colType, charset);

                    if (!tableMap.containsKey(tableName)) {
                        addTable(tableName);
                    }
                    Table table = tableMap.get(tableName);
                    table.addCol(colName);
                    table.addParser(columnParser);
                }
            } catch (SQLException e) {
                System.out.println("database init fail");
            }
            return tableMap;
        },name);
    }

    private void addTable(String tableName) {

        LOGGER.info("Schema load -- DATABASE:{},\tTABLE:{}", name, tableName);

        Table table = new Table(name, tableName);
        tableMap.put(tableName, table);
    }

    public Table getTable(String tableName) {

        return tableMap.get(tableName);
    }
}
