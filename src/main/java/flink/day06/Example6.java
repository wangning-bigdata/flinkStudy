package flink.day06;

import flink.day04.Example10;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 状态后端
 * Created by wangning on 2021/11/22 15:42.
 */
public class Example6 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStateBackend(new FsStateBackend("file:///E:\\IDEAWorkspace\\flinkStudy\\src\\main\\resources\\ckpt", false));
        env.enableCheckpointing(10 * 1000L);

        env.addSource(new Example10.ClickSource())
                .print();

        env.execute();

    }


}
