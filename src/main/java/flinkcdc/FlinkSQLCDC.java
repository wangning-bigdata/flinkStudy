package flinkcdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created by wangning on 2021/11/17 15:43.
 */
public class FlinkSQLCDC {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE user_info (" +
                " id STRING primary key," +
                " name STRING," +
                " sex STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'scan.startup.mode' = 'latest-offset'," +
                " 'hostname' = '10.246.95.137'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = 'Yzoh@123'," +
                " 'database-name' = 'cdc_test'," +
                " 'table-name' = 'user_info'" +
                ")");

        Table table = tableEnv.sqlQuery("select * from user_info");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);

        retractStream
                .print();

        env.execute();


    }
}
