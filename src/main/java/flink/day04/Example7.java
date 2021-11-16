package flink.day04;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * union
 * 1、多条流的合并
 * 2、所有流中的事件类型必须是一致的
 * Created by wangning on 2021/11/16 16:40.
 */
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2);
        DataStreamSource<Integer> stream2 = env.fromElements(3, 4);
        DataStreamSource<Integer> stream3 = env.fromElements(5, 6);

        DataStream<Integer> stream = stream1.union(stream2,stream3);

        stream.print();

        env.execute();
    }
}
