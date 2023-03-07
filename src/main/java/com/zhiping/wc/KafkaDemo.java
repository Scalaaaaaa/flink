package com.zhiping.wc;

import com.zhiping.wc.dto.AvgTemperature;
import com.zhiping.wc.dto.Temperature;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // zk配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "yiyun.mint:9092");
        // 定义kafka数据源
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties);
        env.getConfig().setRestartStrategy(
                RestartStrategies.fixedDelayRestart(5, 5000));
        env.enableCheckpointing(2000);
        // 将kafka数据源 添加到环境, 生成数据流对象
        DataStream<String> sourceStream = env
                .addSource(consumer);
        // 开窗:5分钟一个窗口
        KeyedStream<Temperature, Long> keyedStream = sourceStream.map(item -> {
            return new ObjectMapper().readValue(item, Temperature.class);
        }).keyBy(item -> item.getDeviceId());
        WindowedStream<Temperature, Long, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(5)));
        // tuple3: 设备id, 总温度, 事件个数(数据个数)
        SingleOutputStreamOperator<AvgTemperature> aggregate = windowedStream.aggregate(new AggregateFunction<Temperature, Tuple3<Long, BigDecimal, Integer>, AvgTemperature>() {
            @Override
            public Tuple3<Long, BigDecimal, Integer> createAccumulator() {
                return Tuple3.of(0L, BigDecimal.ZERO, 0);
            }

            @Override
            public Tuple3<Long, BigDecimal, Integer> add(Temperature temperature, Tuple3<Long, BigDecimal, Integer> longBigDecimalIntegerTuple3) {
                longBigDecimalIntegerTuple3.f0 = temperature.getDeviceId();
                longBigDecimalIntegerTuple3.f1 = longBigDecimalIntegerTuple3.f1.add(temperature.getTemperature());
                longBigDecimalIntegerTuple3.f2 = longBigDecimalIntegerTuple3.f2 + 1;
                return longBigDecimalIntegerTuple3;
            }

            @Override
            public AvgTemperature getResult(Tuple3<Long, BigDecimal, Integer> acc) {

                return new AvgTemperature(acc.f0, acc.f1.divide(new BigDecimal(acc.f2), 2, RoundingMode.HALF_UP));
            }

            @Override
            public Tuple3<Long, BigDecimal, Integer> merge(Tuple3<Long, BigDecimal, Integer> acc1, Tuple3<Long, BigDecimal, Integer> acc2) {
                acc1.f1 = acc1.f1.add(acc2.f1);
                acc1.f2 = acc1.f2 + acc2.f2;
                return acc1;
            }
        });
        //DataStream<String> sourceStream = env.addSource(new SimpleStringGenerator());
        aggregate.map(item -> new ObjectMapper().writer().writeValueAsString(item))
                .addSink(new FlinkKafkaProducer010<String>("test", new SimpleStringSchema(), properties));
        env.execute();
    }
}

class SimpleStringGenerator implements SourceFunction<String>, Serializable {

    private static final long serialVersionUID = 1L;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            String str = RandomStringUtils.randomAlphanumeric(5);
            ctx.collect(str);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
