package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface Service<T> {

    void parse(ConsumerRecord<String, T> record);
}
