package kafka.producer;

import kafka.serializer.GsonCustomSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher<T> implements Closeable {
    private final KafkaProducer<String, T> dispatcher;

    KafkaDispatcher() {
        this.dispatcher = new KafkaProducer<>(properties());
    }

    void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        this.dispatcher.send(new ProducerRecord<>(topic, key,value), handleCallbackRecord()).get();
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
        return properties;
    }

    @Override
    public void close() throws IOException {
        this.dispatcher.close();
    }
}
