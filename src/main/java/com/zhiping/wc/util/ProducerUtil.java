package com.zhiping.wc.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

public class ProducerUtil {

    private static ProducerUtil instance = null;
    private KafkaProducer<String, String> producer;
    public static final String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public ProducerUtil() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");// 集群地址
        props.put(ProducerConfig.ACKS_CONFIG, "all");// 请求被认为已完成的标准，all：响应阻塞完整提交。性能上不是最好的选择，但却是最稳的。
        props.put(ProducerConfig.RETRIES_CONFIG, 1);// 失败尝试提交次数
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);// 批量提交大小
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);// 未成批等待提交毫秒数
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client_test");// 发送客户端id身份标识，追查数据来源时比较有用。
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER);// 序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER);// 序列化器
        // 客户端jar版本kafka-clients-0.11.0.1.jar
        this.producer = new KafkaProducer<String, String>(props);
    }

    public static synchronized ProducerUtil getInstance() {
        if (instance == null) {
            instance = new ProducerUtil();
            System.out.println("初始化 kafka producer...");
        }
        return instance;
    }

    // 单条发送
    public void send(ProducerRecord<String, String> msg) {
        producer.send(msg);
    }

    public void send(List<ProducerRecord<String, String>> list) {

        for(int i = 0; i < list.size(); i++){
            ProducerRecord<String, String> record = list.get(i);
            producer.send(record);
        }
        System.out.println("batch send to kafka successfully! num: " + list.size());
        return;
    }

    public void shutdown() {
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {

        ProducerUtil kafkaProducer = new ProducerUtil();
        String topic = "test_topic";
        int sendTimes = 1000;

        long time0 = System.currentTimeMillis();
        String msg = "";
        List<ProducerRecord<String, String>> list = new ArrayList<ProducerRecord<String,String>>();
        for(int i=1;i<=sendTimes;i++){
            msg = "["+i+"]test message";
            ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, msg);
            kafkaProducer.send(data);
        }
        long cost0 = System.currentTimeMillis()-time0;// 发送耗时并不是那么精确，因为LINGER_MS_CONFIG
        System.out.println("发送["+sendTimes+"]条,耗时:"+cost0+"ms.");
    }
}