package flink.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * SELECT * FROM A INNER JOIN B WHERE A.id=B.id;
 * Created by wangning on 2021/11/16 18:36.
 */
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.execute();
    }
}
