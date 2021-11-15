package flink.day01;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 富函数
 * Created by wangning on 2021/11/11 12:45.
 */
public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(1,2,3)
                .map(new RichMapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer integer) throws Exception {
                        return integer * integer;
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("生命周期开始。。。");
                        System.out.println("当前子任务的索引:"+getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("生命周期结束。。。");
                }
                }).print();



        env.execute();
    }
}
