package com.zhiping.wc;

import com.alibaba.fastjson.JSON;
import com.zhiping.wc.dto.Temperature;
import com.zhiping.wc.dto.TemperatureWrapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.util.Properties;

public class MockKafkaProducer {
    public static void main(String[] args) {
        String src = "{\"msg\":{\"ts\":1682066760587,\"temperature\":46.7,\"deviceId\":33}}";
        TemperatureWrapper temperatureWrapper = JSON.parseObject(src, TemperatureWrapper.class);
        System.out.println(temperatureWrapper);
        // zk配置
        /*Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "yiyunmint:9092");
        properties.setProperty("group.id", "shangfei");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaProducer producer = new KafkaProducer<>(properties);
        while (true) {
            *//*Temperature temperature = new Temperature();
            temperature.setTemperature(new BigDecimal(Math.random()*40));
            temperature.setDeviceId(Math.round(Math.random()*100000));*//*
            //new ProducerRecord<Long, String>("temperature", JSON.toJSONString(temperature));
            //producer.send(new ProducerRecord<Long, String>("temperature", JSON.toJSONString(//temperature)));
        }*/
        /*new ProducerRecord<Long, String>("temperature",);
        producer.send()*/
    }
}
