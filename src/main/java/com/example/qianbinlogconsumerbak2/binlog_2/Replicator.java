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

package com.example.qianbinlogconsumerbak2.binlog_2;

import com.example.qianbinlogconsumerbak2.binlog_2.binlog.EventProcessor;
import com.example.qianbinlogconsumerbak2.binlog_2.binlog.Transaction;
import com.example.qianbinlogconsumerbak2.binlog_2.config.Config;
import com.example.qianbinlogconsumerbak2.binlog_2.position.BinlogPosition;
import com.example.qianbinlogconsumerbak2.binlog_2.position.BinlogPositionLogThread;
import com.example.qianbinlogconsumerbak2.binlog_2.productor.RocketMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.qianbinlogconsumerbak2.binlog_2.Test.jsonFileLoadTest;

public class Replicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    private static final Logger POSITION_LOGGER = LoggerFactory.getLogger("PositionLogger");

    private Config config;

    private EventProcessor eventProcessor;

    private RocketMQProducer rocketMQProducer;

    private Object lock = new Object();
    private BinlogPosition nextBinlogPosition;
    private long nextQueueOffset;
    private long xid;

    public static void main(String[] args) {

        Replicator replicator = new Replicator();
        replicator.start();
    }

    public void start() {

        try {
            jsonFileLoadTest();
            // @leimo 1. 加载配置：inputStream -> properties Java对象
            // Spring支持 @ConfigurationProperties注解，但是强耦合Spring IOC机制
            config = new Config();
            config.load();

            // @leimo 2. 启动rocketmq.producer
            // 作用：（1） get binlog position，（2）后续 parse binlog and send binlogEntity to rocketmq
            rocketMQProducer = new RocketMQProducer(config);
            rocketMQProducer.start();

            // @leimo 3. binlogPositionLogThread 定时任务，打印position
            BinlogPositionLogThread binlogPositionLogThread = new BinlogPositionLogThread(this);
            binlogPositionLogThread.start();

            // @leimo 4. 初始化datasource
            eventProcessor = new EventProcessor(this);
            eventProcessor.start();

        } catch (Exception e) {
            LOGGER.error("Start error.", e);
            System.exit(1);
        }
    }



    public void commit(Transaction transaction, boolean isComplete) {
        return;
//        String json = transaction.toJson();
//
//        for (int i = 0; i < 3; i++) {
//            try {
//                // @leimo 1. eventType = xid, 存储position
//                if (isComplete) {
//                    long offset = rocketMQProducer.push(json);
//
//                    synchronized (lock) {
//                        xid = transaction.getXid();
//                        nextBinlogPosition = transaction.getNextBinlogPosition();
//                        nextQueueOffset = offset;
//                    }
//
//                } else {
//                // @leimo 2. evenType != xid, 推送json data 至 rocketmq
//                    rocketMQProducer.push(json);
//                }
//                break;
//
//            } catch (Exception e) {
//                LOGGER.error("Push error,retry:" + (i + 1) + ",", e);
//            }
//        }
    }

    public void logPosition() {

        String binlogFilename = null;
        long xid = 0L;
        long nextPosition = 0L;
        long nextOffset = 0L;

        synchronized (lock) {
            if (nextBinlogPosition != null) {
                xid = this.xid;
                binlogFilename = nextBinlogPosition.getBinlogFilename();
                nextPosition = nextBinlogPosition.getPosition();
                nextOffset = nextQueueOffset;
            }
        }

        if (binlogFilename != null) {
            POSITION_LOGGER.info("XID: {},   BINLOG_FILE: {},   NEXT_POSITION: {},   NEXT_OFFSET: {}",
                xid, binlogFilename, nextPosition, nextOffset);
        }

    }

    public Config getConfig() {
        return config;
    }

    public BinlogPosition getNextBinlogPosition() {
        return nextBinlogPosition;
    }

}
