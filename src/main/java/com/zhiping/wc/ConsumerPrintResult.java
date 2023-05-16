package com.zhiping.wc;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerPrintResult {
    public static void main(String[] args) throws Exception {
        /*final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // zk配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "yiyunmint:9092");
        properties.setProperty("group.id", "testConsumer");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义kafka数据源
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("temperatureResult", new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();
        env.addSource(consumer).print(System.currentTimeMillis()+"-receivedMsg:");
        env.execute();*/
        Properties props = new Properties();

        // 必须设置的属性
        props.put("bootstrap.servers", "yiyunmint:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group1");

        // 可选设置属性
        props.put("enable.auto.commit", "true");
        // 自动提交offset,每1s提交一次
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","latest");
        props.put("client.id", "zy_client_id");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅test1 topic
        consumer.subscribe(Collections.singletonList("temperatureResult"));

        while(true) {
            //  从服务器开始拉取数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.printf("ts=%s, topic = %s ,partition = %d,offset = %d, key = %s, value = %s%n", System.currentTimeMillis(),record.topic(), record.partition(),
                        record.offset(), record.key(), record.value());
            });
        }
    }
}
