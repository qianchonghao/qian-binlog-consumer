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

package com.example.qianbinlogconsumerbak2.binlog_3.position;


import com.example.qianbinlogconsumerbak2.binlog_3.Replicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinlogPositionLogThread extends Thread {
    private Logger logger = LoggerFactory.getLogger(BinlogPositionLogThread.class);

    private Replicator replicator;

    public BinlogPositionLogThread(Replicator replicator) {
        this.replicator = replicator;
        setDaemon(true);
    }

    @Override
    public void run() {

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Offset thread interrupted.", e);
            }

            replicator.logPosition();
        }
    }
}
