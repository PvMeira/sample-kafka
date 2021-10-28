package consumer;

import json.Email;
import json.Order;
import kafka.consumer.ServiceRunner;
import kafka.consumer.ServiceConsumer;
import kafka.desserializar.GsonDeserializer;
import kafka.dto.Message;
import kafka.producer.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class NewOrderMailServiceConsumer implements ServiceConsumer<Order> {

    public static void main(String[] args) {
       new ServiceRunner<>(NewOrderMailServiceConsumer::new).start(1);
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, IOException {
        System.out.println("------------------------------------------");
        try (var emailDispatcher = new KafkaDispatcher<Email>()) {
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL"
                    , record.value().getPayload().getEmail()
                    , new Email("teste@teste.com", "this is the body")
                    , record.value().getId().continueWithId(NewOrderMailServiceConsumer.class.getSimpleName()));
        }


        System.out.println("------------------------------------------");
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return NewOrderMailServiceConsumer.class.getSimpleName();
    }

    @Override
    public Map getCustomProperties() {
        return Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
    }

}
