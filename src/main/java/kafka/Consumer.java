package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created by wangning on 2021/8/19 16:22.
 */
public class Consumer {
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
        consumer.subscribe(Collections.singleton("testww"));
        //kafka的分区逻辑是在poll方法里执行的,所以执行seek方法之前先执行一次poll方法
        //获取当前消费者消费分区的情况
        Set<TopicPartition> assignment = new HashSet<>();

        while (assignment.size() == 0) {
            consumer.poll(Duration.ofSeconds(1));
            assignment = consumer.assignment();
        }
/*
        for (TopicPartition topicPartition : assignment) {
            consumer.seek(topicPartition, 10);
        }
*/

            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(5));
            System.out.println("消息集合是否为空：" + consumerRecords.isEmpty());
            System.out.println("本次拉取的消息数量：" + consumerRecords.count());
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.partition() + "-->消费到的消息key:" + consumerRecord.key() + ",value:" + consumerRecord.value() + ",offset:" + consumerRecord.offset());
                consumer.commitAsync();
            }


    }
}
