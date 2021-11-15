package flink.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * map算子使用
 * Created by wangning on 2021/11/8 15:43.
 */
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    private Boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        while (running) {
                            sourceContext.collect(random.nextInt(1000));
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .map(r -> Tuple2.of(r,r))
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .print();

/*        env
                .addSource(new SourceFunction<Integer>() {
                    private Boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        while (running) {
                            sourceContext.collect(random.nextInt(1000));
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2 map(Integer integer) throws Exception {
                        return Tuple2.of(integer, integer*integer);
                    }
                })
                .print();*/

/*        env
                .addSource(new SourceFunction<Integer>() {
                    private Boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        while (running) {
                            sourceContext.collect(random.nextInt(1000));
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .map(new MyMap())
                .print();*/

        env.execute();
    }

    public static class MyMap implements MapFunction<Integer, Tuple2<Integer, Integer>> {

        @Override
        public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
            return Tuple2.of(integer, integer);
        }
    }
}
