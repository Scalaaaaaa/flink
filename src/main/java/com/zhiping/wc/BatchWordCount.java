package com.zhiping.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 基本不用的dataSet API,1.12之后, 统一采用stream API来写代码,但是, 提交到flink运行的时候,增加运行类型设置为批处理
        //例如:bin/flink run -Dexecution.runtime-mode=BATCH xx.jar
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> ds = env.readTextFile("input/input.txt");
        // 算子 两个泛型,一个输入,一个输出,输入是指  DataSource能提供的输入
        FlatMapOperator<String, Tuple2<String, Integer>> inOut = ds.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        // groupBy有按照字段名分组的,有按照元组顺序分组的.还可以自定义分组器 :KeySelector
        // 含义和 sql里的group by一致
        // 分组的 tuple位置或者字段,都可以是多个
        UnsortedGrouping<Tuple2<String, Integer>> group = inOut.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> sum = group.sum(1);
        sum.print();
    }
}
