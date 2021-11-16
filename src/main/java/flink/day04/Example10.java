package flink.day04;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

/**
 * connect 来连接两条流
 * 1、只能连接两条流
 * 2、两条流中原始类型可以不同
 * Created by wangning on 2021/11/16 17:54.
 */
public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Events> clickStream = env.addSource(new ClickSource());
        DataStreamSource<String> queryStream = env.socketTextStream("localhost", 9999).setParallelism(1);


        clickStream.keyBy(r -> r.user)
                .connect(queryStream.broadcast())
                .flatMap(new CoFlatMapFunction<Events, String, Events>() {
                    private String query = "";


                    @Override
                    public void flatMap1(Events value, Collector<Events> out) throws Exception {
                        if (value.url.equals(query)) {
                            out.collect(value);
                        }
                    }

                    @Override
                    public void flatMap2(String value, Collector<Events> out) throws Exception {
                        query = value;
                    }
                })
                .print();

        env.execute();

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
