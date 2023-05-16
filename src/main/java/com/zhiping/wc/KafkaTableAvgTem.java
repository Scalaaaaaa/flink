package com.zhiping.wc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.math.BigDecimal;
@Slf4j
public class KafkaTableAvgTem {
    public static void main(String[] args) throws Exception {
        // 读命令行参数,参数的值如果包含空格, 则需要用引号引起来
        ParameterTool param = ParameterTool.fromArgs(args);
        // 温度最低值
        String tem = param.get("tem");
        // 最少出现次数
        String lowestCnt = param.get("lowestCnt");
        // 时间窗口 时长
        String interval = param.get("interval");
        // 时间窗口时长的常量值,用于sql
        String timeRange = interval.replace(" ", "-").replace("'","");

        /**
         * x分钟内, 某个设备,如果有n个及以上的温度超过 y度, 则发送报警,报警内容包括:
         * 设备id,超过x度的数据个数,分钟数,时间窗口结束时间
         * 然后就可以根据设备id去查询设备, 数据允许有2迟到时间
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 读书据,度到"表"里,字段: 设备id,温度,时间戳(毫秒).水位线(允许迟到2妙)
        tableEnv.executeSql("CREATE TABLE temperature ( " +
                " deviceId STRING,  tmp DECIMAL(5,2),ts BIGINT, ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3) , " +
                " WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '2' SECOND) " +
                " WITH ( 'connector' = 'kafka'," +
                " 'topic' = 'temperature'," +
                " 'properties.bootstrap.servers' = 'yiyunmint:9092'," +
                " 'properties.group.id' = 'deviceTemperatureWarn'," +
                " 'scan.startup.mode' = 'latest-offset'," +
                " 'format' = 'json')");
        Table sourceTable = tableEnv.from("temperature");
        tableEnv.toDataStream(sourceTable).print("source:");
        // 注册输出表, 输出到  钉钉warn的topic里, 格式是json
        // 输出字段: 设备id,温度x, 超过x度的数据个数,窗口时长,时间窗口结束时间
        tableEnv.executeSql("CREATE TABLE dingTalkWarn ( " +
                " deviceId STRING,  cnt BIGINT,ts TIMESTAMP(3), t_range STRING)  " +
                " WITH ( 'connector' = 'kafka'," +
                " 'topic' = 'dingTalkWarn'," +
                " 'properties.bootstrap.servers' = 'yiyunmint:9092'," +
                " 'format' = 'json')");
        // 开一个x分钟的 滚动窗口, 查询超过 y度的个数大于 n 的 窗口结束时间和设备id
        String insertSql = "INSERT INTO dingTalkWarn " +
                " SELECT deviceId, COUNT(*) AS cnt, window_end, '" + timeRange + "' AS timeRange " +
                " FROM  TABLE( TUMBLE( TABLE temperature,DESCRIPTOR(ts_ltz), INTERVAL " + interval + "))" +
                " WHERE tmp >= " + tem +
                " GROUP BY deviceId,window_start,window_end" +
                " HAVING COUNT(*) >= " + lowestCnt;
        tableEnv.executeSql(insertSql);
        System.out.println(insertSql);
        Table dingTalkWarn = tableEnv.from("dingTalkWarn");
        tableEnv.toDataStream(dingTalkWarn).print("warn     :");

        //tableEnv.toDataStream(tableEnv.from("output")).print("recvData:");
        env.execute();
    }
}
