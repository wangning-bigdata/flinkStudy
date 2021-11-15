package flink.day02;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

/**
 * 字典状态变量
 * Created by wangning on 2021/11/11 17:42.
 */
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> 1)
                .process(new KeyedProcessFunction<Integer, Events, String>() {
                    private MapState<String, Long> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map", Types.STRING, Types.LONG));
                    }

                    @Override
                    public void processElement(Events value, Context ctx, Collector<String> out) throws Exception {
                        if (mapState.contains(value.user)) {
                            mapState.put(value.user, mapState.get(value.user) + 1L);
                        } else {
                            mapState.put(value.user, 1L);
                        }

                        // 求pv平均值
                        Long userNum = 0L;
                        Long pvSum = 0L;
                        for (String user : mapState.keys()) {
                            userNum += 1L;
                            pvSum += mapState.get(user);
                        }
                        out.collect("当前pv的平均值：" + (pvSum* 1.0) / userNum);
                    }
                }).print();

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
