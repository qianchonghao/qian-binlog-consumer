package com.example.qianbinlogconsumerbak2.binlog_bak;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
public class BinlogBootStarter implements InitializingBean {

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    // 仅start依赖 spring机制启动。
    // 其余逻辑，尽量不耦合spring
    // 每处spring机制的耦合，都需要业务应用的改造。
    private void start(){
        /**
         * 1. init and connect datasource
         * 2. get position from datasource and log position by timer
         * 3. init database info and table info
         * 4. binlogClient init
         *      a. eventDeserializer
         *      b. eventListener
         *      c. datasource config set
         * 5.
         *
         */


    }
}
