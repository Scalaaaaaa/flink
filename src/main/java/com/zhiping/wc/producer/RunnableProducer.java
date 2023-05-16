package com.zhiping.wc.producer;

import com.alibaba.fastjson.JSON;
import com.zhiping.wc.dto.Temperature;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Properties;

public class RunnableProducer extends Thread{
    private String topic = "temperature";
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
        int i = 0;
        for(;!stop ; i++){
            Temperature t = new Temperature();
            t.setTs(System.currentTimeMillis());
            BigDecimal bigDecimal = new BigDecimal(Math.random() * 30 + 30);
            t.setTmp(bigDecimal.setScale(2, RoundingMode.CEILING));
            t.setDeviceId((int)Math.round(Math.random()*3));
            String value = JSON.toJSONString(t);
            producer.send(new ProducerRecord<String, String>(topic,
                    Integer.toString(i), value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("exception:"+ exception.getMessage());
                        exception.printStackTrace();
                    }else{
                        System.out.println("callBackSendSuccess:"+metadata.toString());
                    }
                }
            });
            System.out.println("sendSuccess:"+value);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(Thread.currentThread().getName()+"-耗时:"+(end-start));
        System.out.println(Thread.currentThread().getName()+"-一共发送消息数量:"+i);
        producer.close();
    }
}
