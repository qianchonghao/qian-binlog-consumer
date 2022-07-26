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
import com.example.qianbinlogconsumerbak2.binlog_2.util.SqlUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;

public class Schema {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);

    private static final String QUERY_SCHEMA = "select schema_name from information_schema.schemata";

    private static final List<String> IGNORED_DATABASES = new ArrayList<>(
            Arrays.asList(new String[]{"information_schema", "mysql", "performance_schema", "sys"})
    );

    private static List<String> TARGET_DATABASES = Lists.newArrayList();

    private DataSource dataSource;

    private Config config;

    private Map<String, Database> dbMap;

    public Schema(DataSource dataSource, Config config) {
        for (SubscribeInfo info : config.subscribeInfos) {
            TARGET_DATABASES.add(info.getDatabase());
        }
        this.config = config;
        this.dataSource = dataSource;
    }

    public void load() throws SQLException {
        // @leimo 1. 查询 databases，用于遍历table
        dbMap = SqlUtils.executeQuery(dataSource, QUERY_SCHEMA, (rs) -> {
            Map<String,Database> res = Maps.newConcurrentMap();
            try {
                while (rs.next()) {
                    String dbName = rs.getString(1);

                    if (!IGNORED_DATABASES.contains(dbName) && TARGET_DATABASES.contains(dbName)) {
                        Database database = new Database(dbName, dataSource, config);
                        res.put(dbName, database);
                    }
                }

                // @leimo 2. 排除mysql系统db，遍历业务db下的table对象，并解析存储。
                for (Database db : res.values()) {
                    db.init();
                }

                // @leimo todo: 实现 sl4j逻辑
                System.out.println("[Schema]: load success");
            } catch (SQLException e) {
                System.out.println("[Schema]: load fail");
            }
            return res;
        });
    }

    public Table getTable(String dbName, String tableName) {

        if (dbMap == null) {
            reload();
        }

        Database database = dbMap.get(dbName);
        if (database == null) {
            return null;
        }

        Table table = database.getTable(tableName);
        if (table == null) {
            return null;
        }

        return table;
    }

    private void reload() {

        while (true) {
            try {
                load();
                break;
            } catch (Exception e) {
                LOGGER.error("Reload schema error.", e);
            }
        }
    }

    public void reset() {
        dbMap = null;
    }
}
