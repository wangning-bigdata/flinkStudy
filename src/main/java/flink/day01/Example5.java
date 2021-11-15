package flink.day01;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

/**
 * filter算子使用
 * Created by wangning on 2021/11/8 16:15.
 */
public class Example5 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Events> stream = env.addSource(new ClickSource());

/*        stream
                .filter(f -> f.user.equals("Bob"))
                .print();*/

/*        stream.filter(new FilterFunction<Example3.Events>() {
            @Override
            public boolean filter(Example3.Events events) throws Exception {
                return events.user.equals("Bob");
            }
        }).print();*/

//        stream.filter(new MyFilter()).print();


        stream.flatMap(new FlatMapFunction<Events, String>() {
            @Override
            public void flatMap(Events events, Collector<String> collector) throws Exception {
                if(events.user.equals("Bob")){
                    collector.collect(events.user);
                }
            }
        }).print();

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Events> {

        @Override
        public boolean filter(Events events) throws Exception {
            return events.user.equals("Bob");
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
