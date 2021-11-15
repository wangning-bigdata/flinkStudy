package flink.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * KeyedProcessFunction简单使用
 * Created by wangning on 2021/11/11 12:57.
 */
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        stream
                .keyBy(r -> 1)
                .process(new MyKeyed())
                .print();


        env.execute();
    }

    public static class MyKeyed extends KeyedProcessFunction<Integer, String, String> {


        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            //当前机器时间
            long ts = ctx.timerService().currentProcessingTime();
            out.collect("元素:" + value + "在" + new Timestamp(ts) + "到达！");
            //注册一个10s之后的定时器
            long tenSecLater = ts + 10 * 1000L;
            // 注册定时器的语法，注意：注册的是处理时间(机器时间)
            ctx.timerService().registerProcessingTimeTimer(tenSecLater);
        }

        //定时器也是状态
        //每个key独有定时器
        //每个key都可以注册自己的定时器
        //对于每个key，在某个时间戳，只能注册一个定时器
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器触发了。触发时间是："+new Timestamp(timestamp));
        }
    }
}
