package com.limix.bigdata.demo.util;

import java.util.Properties;

/**
 * Kafka工具类
 */
public class KafkaUtil {
    public static String KAFKA_TOPIC_DEMO = "flink_demo";

    public static Properties makeKafkaProperties(boolean isOnline) {
        // 设置kafka相关逻辑
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "demo-flink-group");
        properties.setProperty("auto.offset.reset", "latest");
        properties.put("enable.auto.commit", "true");
        if (isOnline) {
            properties.put("zookeeper.servers",
                    "172.16.112.180:2181,172.16.112.182:2181,172.16.112.184:2181");
            properties.put("bootstrap.servers", "172.16.112.40:9092");
        }
        return properties;
    }
}
