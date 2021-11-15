package flink.day02;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;
import java.util.Random;

/**
 * 连续1s温度上升
 * Created by wangning on 2021/11/11 15:21.
 */
public class Example3 {

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
                            sourceContext.collect(random.nextInt());
                            Thread.sleep(300L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy( r-> 1)
                .process(new IntIncreaseAlert())
                .print();


        env.execute();
    }

    public static class IntIncreaseAlert extends KeyedProcessFunction<Integer,Integer,String>{
        private ValueState<Integer> lastInt;
        private ValueState<Long> timerTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastInt = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("上一次整数值", Types.INT));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("告警时间",Types.LONG));
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void processElement(Integer value, Context context, Collector<String> out) throws Exception {
            Integer prevInt = null;

            if(Objects.nonNull(lastInt.value())){
                prevInt = lastInt.value();
            }

            lastInt.update(value);

            Long ts = null;
            if(Objects.nonNull(timerTs.value())){
                ts = timerTs.value();
            }

            if(Objects.isNull(prevInt)|| value < prevInt){
                if(Objects.nonNull(ts)){
                    context.timerService().deleteProcessingTimeTimer(ts);
                    timerTs.clear();
                }
            }else if(value>prevInt && Objects.isNull(ts)){
                Long oneSeclater = context.timerService().currentProcessingTime() + 1000L;
                context.timerService().registerProcessingTimeTimer(oneSeclater);
                timerTs.update(oneSeclater);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("整数连续1s上升了");
            timerTs.clear();
        }
    }
}
