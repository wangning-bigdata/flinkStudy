package flink.day02;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

/**
 * aggregateFunction 使用
 * 增加聚合函数
 * 缺点：没办法访问窗口的信息
 * Created by wangning on 2021/11/12 15:14.
 */
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Events> stream = env.addSource(new ClickSource());

        stream
                .keyBy(r-> r.user)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg())
                .print();

        env.execute();


    }
    //输入泛型 Events
    //累加器泛型 Integer
    //输出泛型 Integer
    public static class CountAgg implements AggregateFunction<Events,Integer,Integer>{

        //创建累加器
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        //定义累加规则
        @Override
        public Integer add(Events value, Integer accumulator) {
            return accumulator +1;
        }

        //在窗口关闭时返回结果
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        //窗口合并
        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
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
