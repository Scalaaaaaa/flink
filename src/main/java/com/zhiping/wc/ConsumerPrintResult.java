package com.zhiping.wc;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class ConsumerPrintResult {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // zk配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "yiyunmint:9092");
        properties.setProperty("group.id", "testConsumer");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义kafka数据源
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("temperatureResult", new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();
        env.addSource(consumer).print("receivedMsg:");
        env.execute();
    }
}
