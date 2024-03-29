package com.zhiping.wc;

import com.alibaba.fastjson.JSON;
import com.zhiping.wc.dto.Temperature;
import com.zhiping.wc.producer.RunnableProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Producer {
    private static final int threadNum = 1;
    private static final int runSeconds = 7;
    public static void main(String[] args) {
        List<RunnableProducer> list = new ArrayList<RunnableProducer>(10);
        for (int i = 0; i < threadNum; i++) {
            RunnableProducer producer = new RunnableProducer();
            producer.setName("pro-");
            producer.start();
            list.add(producer);
        }
        try {
            Thread.sleep(runSeconds*1000);
            list.forEach(item -> item.stop = true);
            Thread.sleep(4 *1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
