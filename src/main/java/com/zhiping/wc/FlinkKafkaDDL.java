package com.zhiping.wc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkKafkaDDL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE neuron (" +
                        "ts BIGINT," +
                        "node STRING," +
                        //"group STRING," +
                        //"values STRING," +
                        "pt as PROCTIME() " +
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'neuron'," +
                        "'properties.bootstrap.servers' = '139.9.175.10:9092'," +
                        "'properties.group.id' =  'test-consumer-group'," +
                        "'scan.startup.mode' = 'latest-offset'," +
//                "'json.fail-on-missing-field' = 'false'," +
//                "'json.ignore-parse-errors' = 'true'," +
                        "'format' = 'json'" +
                        ")"
        );

        tableEnv.executeSql("CREATE TABLE flinksink (" +
                "ts BIGINT," +
                "nodenum BIGINT," +
                "groupnum BIGINT" +
                //"values STRING" +
                ") WITH (" +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = 'jdbc:mysql://localhost:3306/lib?useSSL=true&characterEncoding=utf8'," +
                "'connector.table' = 'flinksink'," +
                "'connector.driver' =  'com.mysql.cj.jdbc.Driver'," +
                "'connector.username' = 'root'," +
                "'connector.password' = '123456'," +
                "'connector.write.flush.max-rows'='3'\r\n" +
                ")"
        );

        Table result = tableEnv.sqlQuery(
                "SELECT " +
                        "ts as ts, " +                //window_start, window_end,
                        "count(node) as nodenum , count(node) as groupnum " +
                        "FROM TABLE( " +
                        "TUMBLE( TABLE neuron , " +
                        "DESCRIPTOR(pt), " +
                        "INTERVAL '10' SECOND)) " +
                        "GROUP BY ts , window_start, window_end"
        );

//        //方式一：写入数据库
////        result.executeInsert("flinksink").print(); //;.insertInto("flinksink");
//
        //方式二：写入数据库
        tableEnv.createTemporaryView("ResultTable", result);
        tableEnv.executeSql("insert into flinksink SELECT * FROM ResultTable").print();

        env.execute();
    }
}
