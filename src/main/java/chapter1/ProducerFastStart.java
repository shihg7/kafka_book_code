package chapter1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 代码清单1-1
 */
public class ProducerFastStart {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", brokerList);

        // 自己直生产者客户端参数并创建KafkaProducer 实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 构建所需妥发送的消息
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello, Kafka!");
        // 发送消息
        try {
            producer.send(record);
//            producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 关闭生产者客户端
        producer.close();
    }
}
