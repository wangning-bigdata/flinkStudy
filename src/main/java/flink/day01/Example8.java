package flink.day01;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * map keyBy reduce map方式求平均值
 * Created by wangning on 2021/11/11 11:34.
 */
public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env
                .addSource(new SourceFunction<Integer>() {
                    private Boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        while (running) {
                            sourceContext.collect(
                                    random.nextInt(100)
                            );
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                });

        stream.map(r -> Tuple2.of(r, 1))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(r -> true)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0 + value2.f0, value1.f1 + value2.f1);
                    }
                }).map(r -> (r.f0 * 1.0 / r.f1)).print();


        env.execute();

    }


}
