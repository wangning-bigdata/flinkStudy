package flinkcdc.function;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Objects;

/**
 * Created by wangning on 2021/11/17 17:02.
 */
public class CumstomerDeserializationSchema implements DebeziumDeserializationSchema<String> {

    /**
     * {
     * "db":"",
     * "tableName":"",
     * "before":"{"":"",}",
     * "after":"{"":"",}",
     * "op":""
     * }
     *
     * @param sourceRecord
     * @param collector
     * @throws Exception
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建json对象用于封装结果数据
        JSONObject result = new JSONObject();
        //获取库名和表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db", fields[1]);
        result.put("tableName", fields[2]);

        //获取before数据
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (Objects.nonNull(before)) {
            Schema schema = before.schema();
            //获取列名信息
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                beforeJson.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforeJson);

        //获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (Objects.nonNull(after)) {
            Schema schema = after.schema();
            //获取列名信息
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                afterJson.put(field.name(), after.get(field));
            }
        }
        result.put("after", afterJson);

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);

        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
