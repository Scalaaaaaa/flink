package com.zhiping.wc.util;

/*import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhiping.wc.dto.DeviceAvgTemperature;
import com.zhiping.wc.dto.Temperature;
import com.zhiping.wc.dto.TemperatureCnt;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.Key;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;*/

public class KafkaStreamDemo {
    public static void main(String[] args) {
        /*Properties prop =new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG,"temperatureWarnApp");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"yiyun.mint:9092"); //zookeeper的地址
        prop.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,2000);  //提交时间设置为2秒
        //prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,""earliest );   //earliest  latest  none  默认latest
        //prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");  //true(自动提交)  false(手动提交)
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        //创建流构造器
        //hello world
        //hello java
        StreamsBuilder builder = new StreamsBuilder();
        //从kafka中一条一条取温度数据
        String sourceTemplate = "temperatureData";
        KTable<Windowed<Long>, TemperatureCnt> data = builder.stream(sourceTemplate, Consumed.with(Serdes.String(), Serdes.String()))
                .map((k, v) -> KeyValue.pair(k, JSON.parseObject(v, Temperature.class)))
                .selectKey((k,v) -> v.getDeviceId())
                //.map((k, v) -> KeyValue.pair(v.getDeviceId(), v))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5L).toMillis()))
                .aggregate(() -> new TemperatureCnt(BigDecimal.ZERO, 0L), (key, value, aggregate) -> {
                    aggregate.setCnt(aggregate.getCnt() + 1);
                    aggregate.setTotal(aggregate.getTotal().add(value.getTemperature()));
                    return aggregate;
                });

        KStream<Windowed<Long>, TemperatureCnt> windowStream = data.toStream();
        windowStream.peek((k, v)->{
            //为了测试方便，我们将kv输出到控制台
            System.out.println("key:"+k+"   "+"value:"+v);
        });

        windowStream.map((x, y)->{
            return KeyValue.pair(x,
                    JSON.toJSONString(new DeviceAvgTemperature(x.key(),
                            y.getTotal().divide(new BigDecimal(y.getCnt()),2, RoundingMode.HALF_UP))));
        }).to("wordcount-output");

        final Topology topo=builder.build();
        final KafkaStreams streams = new KafkaStreams(topo, prop);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("stream"){
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);*/
    }
}
