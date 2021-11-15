package flink.day02;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;
import java.util.Random;

/**
 * 状态变量  求平均值
 * Created by wangning on 2021/11/11 14:26.
 */
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.addSource(new SourceFunction<Integer>() {
            private Boolean running = true;
            Random random = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (running) {
                    sourceContext.collect(random.nextInt(10));
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        stream.keyBy(r -> true)
                .process(new KeyedProcessFunction<Boolean, Integer, Double>() {
                    //声明一个状态变量作为累加器
                    //状态变量的可见范围是当前key
                    //状态变量是单例，只能被实例化一次
                    private ValueState<Tuple2<Integer, Integer>> valueState;

                    private ValueState<Long> timeTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //实例化状态变量
                        valueState = getRuntimeContext().getState(
                                //状态描述符
                                new ValueStateDescriptor<Tuple2<Integer, Integer>>("sum-count", Types.TUPLE(Types.INT, Types.INT))
                        );
                        timeTs = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("timer", Types.LONG)
                        );
                    }

                    @Override
                    public void processElement(Integer value, Context context, Collector<Double> out) throws Exception {
                        //当第一条数据到来时，状态变量为null
                        //使用.value()方法读取状态变量的值，使用.update()方法更新状态变量的值
                        if (Objects.isNull(valueState.value())) {
                            valueState.update(Tuple2.of(value, 1));
                        } else {
                            Tuple2<Integer, Integer> tmp = valueState.value();
                            valueState.update(Tuple2.of(tmp.f0 + value, tmp.f1 + 1));
                        }

                        if (Objects.isNull(timeTs.value())) {
                            long time = context.timerService().currentProcessingTime() + 10 * 1000L;
                            context.timerService().registerProcessingTimeTimer(time);
                            timeTs.update(time);
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Double> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (Objects.nonNull(valueState)) {
                            out.collect((valueState.value().f0 * 1.0) / valueState.value().f1);
                            timeTs.clear();
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }

                }).print();


        env.execute();
    }
}
