package com.zhiping.wc;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSink extends
        RichSinkFunction<Tuple3<Integer, String, Integer>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    String username = "";
    String password = "";
    String drivername = "";   //
    String dburl = "";

    @Override
    public void invoke(Tuple3<Integer, String, Integer> value) throws Exception {
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "replace into table(id,num,price) values(?,?,?)"; //
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1, value.f0);
        preparedStatement.setString(2, value.f1);
        preparedStatement.setInt(3, value.f2);
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}