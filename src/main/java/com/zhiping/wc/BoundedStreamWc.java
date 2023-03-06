package com.zhiping.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamWc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.readTextFile("input/input.text");
        SingleOutputStreamOperator<Tuple2<String, Long>> opr = dss.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            for (String item :
                    line.split(" ")) {
                out.collect(Tuple2.of(item, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> ks = opr.keyBy(data -> data.f0);
        // 位置是从0开始的,那么这段代码的逻辑是, 按照f0分组,每组对 下标为1的tuple元素求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = ks.sum(1);
        sum.print();
        // 以上是流处理的定义, 定义好了,要启动才会执行
        env.execute();
    }
}
