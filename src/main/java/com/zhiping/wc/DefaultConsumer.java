package com.zhiping.wc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

//@Service
@Slf4j
public class DefaultConsumer {
    private static String topic = "dingTalkWarn";
    public static void main(String[] args) {
        init();
    }
    //@PostConstruct
    public static void init(){
        log.info("starting vertx kafka consumer....");
        Properties config = new Properties();
        config.put("bootstrap.servers", "yiyunmint:9092");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", "my_group");
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "false");

        // 使用消费者和 Apache Kafka 交互
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(config);
        consumer.subscribe(Arrays.asList(topic));
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 30 *1000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(record -> process(record));
            consumer.commitSync();
        }
        consumer.close();
    }

    public static void process(ConsumerRecord<String, String> record) {
        log.info(record.value());
    }
}
