package flink.day02;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

/**
 * 使用KeyedProcessFunction模拟5秒的滚动窗口
 * 模拟的是增量聚合函数和全窗口聚合函数结合使用的情况
 * Created by wangning on 2021/11/12 15:47.
 */
public class Example9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Events> stream = env.addSource(new ClickSource());

        stream.keyBy(r -> r.user)
                .process(new MyWindow())
                .print();


        env.execute();
    }

    public static class MyWindow extends KeyedProcessFunction<String, Events, String> {

        //key是窗口的开始时间，value是窗口中的pv数值
        private MapState<Long, Integer> mapState;
        //窗口大小
        private Long windowSize = 5000L;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Integer>("windowStart-pvCount", Types.LONG, Types.INT));
        }

        @Override
        public void processElement(Events value, Context ctx, Collector<String> out) throws Exception {
            //计算当前元素所属的窗口的开始时间
            long currTime = ctx.timerService().currentProcessingTime();
            long windowStart = currTime - currTime % windowSize;
            long windowEnd = windowStart + windowSize;

            if(mapState.contains(windowStart)){
                mapState.put(windowStart,mapState.get(windowStart)+1);
            }else {
                mapState.put(windowStart,1);
            }

            ctx.timerService().registerProcessingTimeTimer(windowEnd-1L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            long windowEnd = timestamp +1L;
            long windowStart = windowEnd - windowSize;
            out.collect("用户: " + ctx.getCurrentKey() + "在窗口" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "中的pv次数是" + mapState.get(windowStart));

            mapState.remove(windowStart);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

    public static class ClickSource implements SourceFunction<Events> {
        private Boolean running = true;
        private String[] userArr = {"Mary", "Bob", "Alice", "Liz"};
        private String[] urlArr = {"baidu.com", "taobao.com", "jd.com", "bianfeng.com", "zhihu.com"};
        private Random random = new Random();

        @Override
        public void run(SourceContext<Events> sourceContext) throws Exception {
            while (running) {
                sourceContext.collect(
                        new Events(
                                userArr[random.nextInt(userArr.length)],
                                urlArr[random.nextInt(urlArr.length)],
                                Calendar.getInstance().getTimeInMillis()
                        )
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class Events {
        public String user;
        public String url;
        public Long timestamp;

        public Events() {
        }

        public Events(String user, String url, Long timestamp) {
            this.user = user;
            this.url = url;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Events{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
}
