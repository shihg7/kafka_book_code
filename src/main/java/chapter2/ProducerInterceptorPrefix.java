package chapter2;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 代码清单2-6
 * 生产者拦截器
 */
public class ProducerInterceptorPrefix implements
        ProducerInterceptor<String, String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    // onSend（）方法来对消息进行相应的定制化操作
    @Override
    public ProducerRecord<String, String> onSend(
            ProducerRecord<String, String> record) {
        String modifiedValue = "prefix1-" + record.value();
        return new ProducerRecord<>(record.topic(),
                record.partition(), record.timestamp(),
                record.key(), modifiedValue, record.headers());
//        if (record.value().length() < 5) {
//            throw new RuntimeException();
//        }
//        return record;
    }

    // KafkaProducer 会在消息被应答（ Acknowledgement ）之前或消息发送失败时调用生产者拦
    // 截器的onAcknowledgement（）方法，优先于用户设定的Callback 之前执行。
    @Override
    public void onAcknowledgement(
            RecordMetadata recordMetadata,
            Exception e) {
        if (e == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    // close（）方法主要用于在关闭拦截器时执行一些资源的清理工作。
    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
        System.out.println("[INFO] 发送成功率="
                + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}
