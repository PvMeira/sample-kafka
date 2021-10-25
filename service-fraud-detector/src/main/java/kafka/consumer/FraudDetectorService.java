package kafka.consumer;

import json.Order;
import kafka.desserializar.GsonDeserializer;
import kafka.producer.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import utils.FraudUtils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
    public void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("------------------------------------------");
        System.out.println("Processing order");
        System.out.println("Order ID for this request was : ".concat(record.value().getOrderId()));

        if (FraudUtils.isSuspectedOrder(record)) {
            System.out.println("Order is a Fraud");
            this.sendMessage("ECOMMERCE_ORDER_REJECT", record.value().getEmail(), record.value());
        } else {
            System.out.println("Order processed with success");
            this.sendMessage("ECOMMERCE_ORDER_APPROVED", record.value().getEmail(), record.value());

        }
        System.out.println("------------------------------------------");
    }


    private void sendMessage(String topic, String key, Order order) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            orderDispatcher.send(topic, key, order);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
