package kafka.consumer;

import json.Email;
import kafka.desserializar.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class EmailService implements Service<Email> {
    public static void main(String[] args) {
        var emailService = new EmailService();
        try (var service = new KafkaService<>(  EmailService.class.getSimpleName(),
                                        "ECOMMERCE_SEND_EMAIL",
                                              emailService::parse,
                Email.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()))) {
            service.run();
        }
    }

    @Override
    public void parse(ConsumerRecord<String, Email> record) {
        System.out.println("------------------------------------------");
        System.out.println("Send email");
        System.out.println(record.key());
        System.out.println("Email sent");
        System.out.println("------------------------------------------");
    }
}
