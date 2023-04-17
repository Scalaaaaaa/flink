package com.zhiping.wc.producer;

import com.alibaba.fastjson.JSON;
import com.zhiping.wc.dto.Temperature;
import javolution.io.Struct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class RunnableProducer extends Thread{
    public Boolean stop = false;
    @Override
    public void run() {
        Properties props = new Properties();
        //kafka集群，broker-list
        props.put("bootstrap.servers","yiyunmint:9092");
        props.put("acks","all");
        props.put("retries",1);  //重试次数
        props.put("batch.size",16384); //批次大小
        props.put("linger.mx",1);  //等待时间
        props.put("buffer.memory", 33554432); ///RecordAccumulator 缓冲区大小

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
        long start = System.currentTimeMillis();
        int i = 1;
        for(;!stop ; i++){
            Temperature t = new Temperature();
            t.setTs(System.currentTimeMillis());
            t.setTemperature(Math.random() * 30 + 10);
            t.setDeviceId((int)Math.round(Math.random()*100));
            producer.send(new ProducerRecord<String, String>("temperature",
                    Integer.toString(i), JSON.toJSONString(t)));
        }
        long end = System.currentTimeMillis();
        System.out.println(Thread.currentThread().getName()+"-耗时:"+(end-start));
        System.out.println(Thread.currentThread().getName()+"-一共发送消息数量:"+i);
        producer.close();
    }
}
