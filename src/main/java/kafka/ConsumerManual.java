package kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * Created by wangning on 2021/8/19 16:22.
 */
public class ConsumerManual {
    private Map<TopicPartition,Long> offset = new HashMap<>();
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_example");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumerId2");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "3.227.34.247:9092,34.203.185.50:9092,34.194.35.71:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("testww"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commit();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        });


    }

    /**
     * 将缓存中的offset提交到自定义介质中
     */
    private static void commit() {

    }
}
