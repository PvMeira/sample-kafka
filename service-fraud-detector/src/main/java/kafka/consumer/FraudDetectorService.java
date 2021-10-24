package kafka.consumer;

import json.Order;
import kafka.desserializar.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FraudDetectorService implements Service<Order> {
    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        try (var service = new KafkaService<>(  FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Order.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()))) {
            service.run();
        }
    }

    @Override
    public void parse(ConsumerRecord<String, Order> record) {
        System.out.println("------------------------------------------");
        System.out.println("Processing order");
        System.out.println(record.key());
        System.out.println("Order ID for this request was : ".concat(record.value().getOrderId()));
        System.out.println("Order processed with success");
        System.out.println("------------------------------------------");
    }
}
