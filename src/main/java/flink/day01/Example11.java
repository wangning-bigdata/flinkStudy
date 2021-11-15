package flink.day01;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * 自定义输出类型
 * Created by wangning on 2021/11/11 12:57.
 */
public class Example11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        env.fromElements(1,2,3,4)
                .addSink(new SinkFunction<Integer>() {
                    @Override
                    public void invoke(Integer value, Context context) throws Exception {
                        System.out.println(value);
                    }
                });

        env.execute();
    }
}
