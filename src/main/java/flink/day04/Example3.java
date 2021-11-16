package flink.day04;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 迟到元素发送到侧输出流
 * Created by wangning on 2021/11/16 14:45.
 */
public class Example3 {
    //定义侧输出流的标签
    private static OutputTag<String> lateElement = new OutputTag<String>("late-element") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                //指定时间戳 发送数据
                ctx.collectWithTimestamp(Tuple2.of("hello world", 1000L), 1000L);
                //发送水位线
                ctx.emitWatermark(new Watermark(999L));

                ctx.collectWithTimestamp(Tuple2.of("hello flink", 2000L), 2000L);
                ctx.emitWatermark(new Watermark(1999L));
                ctx.collectWithTimestamp(Tuple2.of("hello late", 1000L), 1000L);
            }

            @Override
            public void cancel() {

            }
        })
                .process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        long currentWatermark = ctx.timerService().currentWatermark();
                        out.collect("当前水位线："+currentWatermark);
                        if (value.f1 < currentWatermark) {
                            ctx.output(lateElement, "迟到元素发送到侧输出流：" + value);
                        } else {
                            out.collect("正常达到的元素：" + value);
                        }
                    }
                });


        result.print("主流数据：");

        result.getSideOutput(lateElement).print("侧输出流：");

        env.execute();
    }
}
