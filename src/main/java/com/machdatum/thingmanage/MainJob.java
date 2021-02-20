package com.machdatum.thingmanage;

import java.lang.String;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public final class MainJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,settings);
        tEnv.executeSql("CREATE TABLE source("
                + "Cnt INT ,Ts TIMESTAMP(3) METADATA FROM 'timestamp' ,Device INT "
                + ")"
                + "WITH"
                + "("
                + "'connector' = 'kafka',"
                + "'topic' = 'rawdata',"
                + "'properties.bootstrap.servers' = '192.168.1.130:29092',"
                + "'properties.group.id' = 'testGroup',"
                + "'scan.startup.mode' = 'EARLIEST',"
                + "'format' = 'json'"
                + ")");
    }

    private static final Table Window1(StreamTableEnvironment tEnv, Table source) {
        Table table = source.window(Tumble.over("1.minute").on("Ts").as("w")).groupBy("[w, Device]").select("Device,w.start AS wStart,w.end AS wEnd,(MAX(Cnt) - MIN(Cnt)) AS Cnt");
        tEnv.registerTable("Window1", table);
        return table;
    }
}