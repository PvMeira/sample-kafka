package kafka.consumer;

import kafka.desserializar.GsonDeserializer;
import kafka.dto.Message;
import kafka.producer.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final ConsumerFunction<T> parse;
    private final KafkaConsumer<String, Message<T>> consumer;
    private final KafkaDispatcher<T> deadLetter = new KafkaDispatcher<>();


    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse,  Map<String, String> customProperties) {
        this(groupId, parse, customProperties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> customProperties) {
        this(groupId, parse, customProperties);
        consumer.subscribe(topic);
    }

    private KafkaService(String groupId, ConsumerFunction<T> parse, Map<String, String> customProperties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(this.properties(groupId, customProperties));
    }

    public void run() throws ExecutionException, InterruptedException {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                for (var record : records) {
                    try {
                        parse.consume(record);
                    } catch (Exception e) {
                        //var dispatcher = KafkaDispatcher<>();
                        e.printStackTrace();
                        System.out.println("Error on consuming the message");
                        deadLetter.send("ECOMMERCE_SEND_EMAIL"
                                , record.value().getId().toString()
                                , record.value().getPayload()
                                , record.value().getId().continueWithId("DEADLETTER"));

                    }
                }
            }
        }
    }

    private  Properties properties(String groupId, Map<String, String> customProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        properties.putAll(customProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
