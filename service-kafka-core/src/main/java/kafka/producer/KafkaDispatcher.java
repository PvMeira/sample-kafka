package kafka.producer;

import kafka.dto.CorrelationID;
import kafka.dto.Message;
import kafka.serializer.GsonCustomSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> implements Closeable {
    private final KafkaProducer<String, Message<T>> dispatcher;

    public KafkaDispatcher() {
        this.dispatcher = new KafkaProducer<>(properties());
    }

    public void send(String topic, String key, T value, CorrelationID id) throws ExecutionException, InterruptedException {
        var future = sendAsync(topic,key, value, id);
        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, T value, CorrelationID id) {
        var payload = new Message<>(id.continueWithId("__" + topic), value);
        var record = new ProducerRecord<>(topic, key, payload);
        System.out.println("Sending Message with ID : " + id);
        return this.dispatcher.send(record, handleCallbackRecord());
    }

    private static Callback handleCallbackRecord() {
        return (var1, var2) -> {
            if (var2 != null) {
                var2.printStackTrace();
                return;
            }
            System.out.println("::::::::::::::::::::::::::::::");
            System.out.println("Message was send to ".concat(var1.topic()));
            System.out.println("::::::::::::::::::::::::::::::");
        };
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonCustomSerializer.class.getName());
        // way more reliable way to send messages on a cluster, this will wait the leader replicate all the messages to the others brokers
        // Slow but full reliable
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    @Override
    public void close() throws IOException {
        this.dispatcher.close();
    }
}
