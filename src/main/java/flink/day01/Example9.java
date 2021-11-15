package flink.day01;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试
 * shuffle 随机分到不同的插槽
 * rebalance 按顺产分到不同的插槽
 * broadcast 复制一份分到不同的插槽
 * Created by wangning on 2021/11/11 12:09.
 */
public class Example9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

/*        env
                .fromElements(1,2,3,4,5,6).setParallelism(1)
                .shuffle()
                .print("shuffle").setParallelism(2);*/

/*        env
                .fromElements(1,2,3,4,5,6).setParallelism(1)
                .rebalance()
                .print("rebalance").setParallelism(2);*/

        env
                .fromElements(1,2,3,4,5,6).setParallelism(1)
                .broadcast()
                .print("broadcast").setParallelism(2);


        env.execute();
    }
}
