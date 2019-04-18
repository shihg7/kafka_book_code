package chapter2;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 代码清单2-1
 */
public class KafkaProducerAnalysis {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id", "producer.client.id.demo");
        return props;
    }

    // 以下的写法比上面更好
    public static Properties initNewConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return props;
    }

    // 以下的写法最好，代码简洁
    public static Properties initPerferConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return props;
    }

    public static void main(String[] args) throws InterruptedException {
        Properties props = initPerferConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

//        KafkaProducer<String, String> producer = new KafkaProducer<>(props,
//                new StringSerializer(), new StringSerializer());


       /* ProducerRecord类的定义如下
       public class ProducerRecord<K, V> {
            private final String topic; //主题
            private final Integer partition; //分区号
            private final Headers headers; //消息头部
            private final K key; //键
            private final V value; //值
            private final Long timestamp; //消息的时间戳
            //省略其他成员方法和构造方法
        */

        // 构造方法有多种，下面的最简单把别的属性值置为null
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello, Kafka!");
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 立刻回收资源
        // producer.close();

        // 带超时时间的close()
        producer.close(30, TimeUnit.SECONDS);
    }

    // 发后即忘
    public void fire_and_forget(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 同步1
    public void sync1(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            producer.send(record).get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 同步2,send（）方法本身就是异步的， send（）方法返回的Future 对象可以使调用方稍后获得发送的结果
    public void sync2(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println(metadata.partition() + ":" + metadata.offset());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 异步，Kafka 有响应时就会回调， 要么发送成功，要么抛出异常
    public void async(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            // 重写异步send
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + ":" + metadata.offset());
                    }
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
