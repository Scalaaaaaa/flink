package com.zhiping.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWc {
    public static void main(String[] args) throws Exception {
        ParameterTool argTool = ParameterTool.fromArgs(args);
        // 参数传递 --key value --key1 value1
        // flinkClient 把 待执行的jar 提交给JobManager, JobManager分发给TaskManager
        argTool.get("host");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Tuple2<String, Long>> opr = dss.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            for (String item :
                    line.split(" ")) {
                out.collect(Tuple2.of(item, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> ks = opr.keyBy(data -> data.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = ks.sum(1);
        sum.print();
        // 以上是流处理的定义, 定义好了,要启动才会执行
        env.execute();
    }
}
