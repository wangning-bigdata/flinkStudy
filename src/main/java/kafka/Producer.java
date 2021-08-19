package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by wangning on 2021/8/19 14:04.
 */
public class Producer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {


        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("client.id", "example");
        properties.put("bootstrap.servers", "3.227.34.247:9092,34.203.185.50:9092,34.194.35.71:9092");
        properties.put("acks", "all");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);


        for (int i = 0; i <= 10; i++) {
            String value = "value" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>("testww", Integer.toString(i * 2), value);
            Future<RecordMetadata> send = kafkaProducer.send(record);
            RecordMetadata recordMetadata = send.get();
            System.out.println(recordMetadata);
            System.out.println(i + "条发送成功！");
        }

        kafkaProducer.close();


    }
}
