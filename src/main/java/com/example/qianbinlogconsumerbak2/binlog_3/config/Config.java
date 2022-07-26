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

package com.example.qianbinlogconsumerbak2.binlog_3.config;

import com.alibaba.fastjson.JSONObject;
import org.springframework.util.ClassUtils;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;


public class Config {

    public String mysqlAddr;
    public Integer mysqlPort;
    public String mysqlUsername;
    public String mysqlPassword;
    public String group;


    public List<SubscribeInfo> subscribeInfos;
    public String mqNamesrvAddr;
    public String mqTopic;

    public String startType = "DEFAULT";
    public String binlogFilename;
    public Long nextPosition;
    public Integer maxTransactionRows = 100;

    public void load() throws Exception {

        InputStream in = Config.class.getClassLoader().getResourceAsStream("rocketmq_mysql.conf");
//        InputStream in = Config.class.getClassLoader().getResourceAsStream("application.yml");
        Properties properties = new Properties();
        properties.load(in);

        properties2Object(properties, this);

    }

    private void properties2Object(final Properties p, final Object object) throws InstantiationException, IllegalAccessException {
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String mn = method.getName();
            if (mn.startsWith("set")) {
                try {
                    String tmp = mn.substring(4);
                    String first = mn.substring(3, 4);

                    String key = first.toLowerCase() + tmp;
                    String property = p.getProperty(key);
                    if (property != null) {
                        Class<?>[] pt = method.getParameterTypes();
                        if (pt != null && pt.length > 0) {
                            String cn = pt[0].getSimpleName();
                            Object arg;
                            if (cn.equals("int") || cn.equals("Integer")) {
                                arg = Integer.parseInt(property);
                            } else if (cn.equals("long") || cn.equals("Long")) {
                                arg = Long.parseLong(property);
                            } else if (cn.equals("double") || cn.equals("Double")) {
                                arg = Double.parseDouble(property);
                            } else if (cn.equals("boolean") || cn.equals("Boolean")) {
                                arg = Boolean.parseBoolean(property);
                            } else if (cn.equals("float") || cn.equals("Float")) {
                                arg = Float.parseFloat(property);
                            } else if (cn.equals("String")) {
                                arg = property;
                            } else{
                                // @leimo todo : 思考有没有 解析的框架
                                // @leimo todo: 获取不到泛型。 通过注解获取
                                ClassInfo classInfo = method.getAnnotation(ClassInfo.class);
                                if (classInfo != null) {
                                    Class fieldType = ClassUtils.forName(classInfo.className(), this.getClass().getClassLoader());
                                    arg = ClassInfo.ARRAY.equals(classInfo.type()) ?
                                            JSONObject.parseArray(property, fieldType) :
                                            JSONObject.parseObject(property, fieldType);
                                } else {
                                    continue;
                                }
                            }
                            method.invoke(object, arg);
                        }
                    }
                } catch (Throwable ignored) {
                }
            }
        }
    }

    @ClassInfo(className = "com.example.qianbinlogconsumerbak2.binlog_3.config.SubscribeInfo",type = ClassInfo.ARRAY)
    public void setSubscribeInfos(List<SubscribeInfo> subscribeInfos) {
        this.subscribeInfos = subscribeInfos;
    }

    public void setMysqlAddr(String mysqlAddr) {
        this.mysqlAddr = mysqlAddr;
    }

    public void setMysqlPort(Integer mysqlPort) {
        this.mysqlPort = mysqlPort;
    }

    public void setMysqlUsername(String mysqlUsername) {
        this.mysqlUsername = mysqlUsername;
    }

    public void setMysqlPassword(String mysqlPassword) {
        this.mysqlPassword = mysqlPassword;
    }

    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    public void setNextPosition(Long nextPosition) {
        this.nextPosition = nextPosition;
    }

    public void setMaxTransactionRows(Integer maxTransactionRows) {
        this.maxTransactionRows = maxTransactionRows;
    }

    public void setMqNamesrvAddr(String mqNamesrvAddr) {
        this.mqNamesrvAddr = mqNamesrvAddr;
    }

    public void setMqTopic(String mqTopic) {
        this.mqTopic = mqTopic;
    }

    public void setStartType(String startType) {
        this.startType = startType;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}