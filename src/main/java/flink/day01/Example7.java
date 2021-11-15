package flink.day01;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * sum 滚动聚合
 * Created by wangning on 2021/11/11 11:15.
 */
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<Integer, Integer>> stream = env.fromElements(
                Tuple2.of(1, 3),
                Tuple2.of(1, 4),
                Tuple2.of(1, 2),
                Tuple2.of(1, 7)
        );


        KeyedStream<Tuple2<Integer, Integer>, Integer> keyedStream = stream.keyBy(r -> r.f0);

//        keyedStream.sum(1).print();


        keyedStream.reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0,value1.f1+value2.f1);
            }
        }).print();


/*
        keyedStream.reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0,Math.max(t1.f1,t2.f1));
            }
        }).print();
*/

        env.execute();
    }
}
