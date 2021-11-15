package flink.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flatMap算子使用
 * Created by wangning on 2021/11/8 16:47.
 */
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> stream = env.fromElements("white", "black", "green");

/*        stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                if (s.equals("white")) {
                    collector.collect(s);
                } else if (s.equals("green")) {
                    collector.collect(s);
                    collector.collect(s);
                }
            }
        }).print();*/
        stream.flatMap((String s, Collector<String> out) -> {
            if (s.equals("white")) {
                out.collect(s);
            } else if (s.equals("green")) {
                out.collect(s);
                out.collect(s);
            }
        })
                .returns(Types.STRING).print();

        env.execute();
    }
}
