package kafka.consumer;

import json.Email;
import kafka.desserializar.GsonDeserializer;
import kafka.dto.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class EmailServiceConsumer implements ServiceConsumer<Email> {
    public static void main(String[] args) {
        new ServiceRunner(EmailServiceConsumer::new).start(5);
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Email>> record) {
        System.out.println("------------------------------------------");
        System.out.println("Send email");
        System.out.println(record.key());
        System.out.println("Email sent");
        System.out.println("------------------------------------------");
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    @Override
    public String getConsumerGroup() {
        return EmailServiceConsumer.class.getSimpleName();
    }

    @Override
    public Map getCustomProperties() {
        return Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
    }
}
