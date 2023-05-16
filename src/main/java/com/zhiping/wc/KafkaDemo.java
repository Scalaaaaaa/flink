package com.zhiping.wc;
import com.alibaba.fastjson.JSON;
import com.zhiping.wc.algrithim.DeviceIdAvgTemperature;
import com.zhiping.wc.dto.DeviceAvgTemperature;
import com.zhiping.wc.dto.Temperature;
import com.zhiping.wc.dto.TemperatureWrapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.time.Duration;
import java.util.Properties;

public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // zk配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "yiyunmint:9092");
        properties.setProperty("group.id", "shangfei");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //Pattern pattern = Pattern.compile("avgWarn-.*-direct");
        // 定义kafka数据源
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("temperature", new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();
        //consumer.
        /*env.getConfig().setRestartStrategy(
                RestartStrategies.fixedDelayRestart(5, 5000));*/
        env.getConfig().setAutoWatermarkInterval(1000);
        env.enableCheckpointing(2000);
        // 将kafka数据源 添加到环境, 生成数据流对象, 以数据处理时间作为时间戳来分窗口
        DataStream<String> sourceStream = env
                .addSource(consumer);
        sourceStream.print("directPrint:");
        // 开窗:5分钟一个窗口
        KeyedStream<Temperature, Long> keyedStream = sourceStream
                .map(item -> JSON.parseObject(item, TemperatureWrapper.class).getMsg())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Temperature>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Temperature>() {
                    @Override
                    public long extractTimestamp(Temperature element, long recordTimestamp) {
                        return element.getTs();
                    }
                }))
                .keyBy(item -> item.getDeviceId().longValue());
        WindowedStream<Temperature, Long, TimeWindow> windowedStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(3)));
        // tuple3: 设备id, 总温度, 事件个数(数据个数)
        SingleOutputStreamOperator<DeviceAvgTemperature> aggregate = windowedStream
                .aggregate(new DeviceIdAvgTemperature());
        aggregate.print("avgTemp:");
        SingleOutputStreamOperator<String> resultStream = aggregate
            .map(item -> new ObjectMapper().writer().writeValueAsString(item));
        resultStream
                .addSink(new FlinkKafkaProducer<String>("yiyunmint:9092","toWebsocket", new SimpleStringSchema()));
        env.execute();
    }
}


/*class SimpleStringGenerator implements SourceFunction<String>, Serializable {

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
}*/
