package com.example.qianbinlogconsumerbak2.binlog_3;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.example.qianbinlogconsumerbak2.binlog_3.config.SubscribeInfo;
import com.google.common.io.CharStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class Test {

    public static void jsonFileLoadTest() {
        // 1. json file -> inputstream -> String -> parseObject     // com.google.common.io
        InputStream in = Replicator.class.getClassLoader().getResourceAsStream("demo.json");
        try {
            String content = CharStreams.toString(new InputStreamReader(in));
            // 2. TypeReference 传递泛型参数
            // https://cloud.tencent.com/developer/article/1870182
            // new TypeReference(){} 创建TypeReference的匿名子类.存储泛型信息
            List<SubscribeInfo> infos = JSONObject.parseObject(content,new TypeReference<List<SubscribeInfo>>(){});
            System.out.println("test success");
        } catch (IOException e) {
            System.out.println(" read json fail");
        }
    }
}
