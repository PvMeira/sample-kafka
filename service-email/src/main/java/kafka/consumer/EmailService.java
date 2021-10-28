package kafka.consumer;

import json.Email;
import kafka.desserializar.GsonDeserializer;
import kafka.dto.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailService implements Service<Message<Email>> {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailService = new EmailService();
        try (var service = new KafkaService<>(  EmailService.class.getSimpleName(),
                                        "ECOMMERCE_SEND_EMAIL",
                                              emailService::parse,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()))) {
            service.run();
        }
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Email>> record) {
        System.out.println("------------------------------------------");
        System.out.println("Send email");
        System.out.println(record.key());
        System.out.println("Email sent");
        System.out.println("------------------------------------------");
    }
}
