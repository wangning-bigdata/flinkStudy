package flinkcdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import flinkcdc.function.CumstomerDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义反序列化
 * Created by wangning on 2021/11/17 13:12.
 */
public class FlinkCDC2 {
    public static void main(String[] args) throws Exception {

        //1.获取flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //1.1开启ck
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.setStateBackend(new FsStateBackend("file:///E:\\flink\\checkpoints"));


        // 通过flinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("10.246.95.137")
                .port(3306)
                .username("root")
                .password("Yzoh@123")
                .databaseList("cdc_test")
//                .tableList("cdc_test.user_info")
                .deserializer(new CumstomerDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> stream = env.addSource(sourceFunction);

        stream.print();



        env.execute();

    }
}
