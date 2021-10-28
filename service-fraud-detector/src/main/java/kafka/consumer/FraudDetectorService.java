package kafka.consumer;

import json.Order;
import kafka.desserializar.GsonDeserializer;
import kafka.dto.CorrelationID;
import kafka.dto.Message;
import kafka.producer.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import utils.FraudUtils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements Service<Message<Order>> {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var fraudDetectorService = new FraudDetectorService();
        try (var service = new KafkaService<>(  FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()))) {
            service.run();
        }
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("------------------------------------------");
        System.out.println("Processing order");
        System.out.println("Order ID for this request was : ".concat(record.value().getPayload().getOrderId()));

        if (FraudUtils.isSuspectedOrder(record.value().getPayload())) {
            System.out.println("Order is a Fraud");
            this.sendMessage("ECOMMERCE_ORDER_REJECT", record.value().getPayload().getEmail(), record.value().getPayload(), record.value().getId());
        } else {
            System.out.println("Order processed with success");
            this.sendMessage("ECOMMERCE_ORDER_APPROVED", record.value().getPayload().getEmail(), record.value().getPayload(), record.value().getId());

        }
        System.out.println("------------------------------------------");
    }


    private void sendMessage(String topic, String key, Order order, CorrelationID id) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            orderDispatcher.send(topic, key, order, id.continueWithId(FraudDetectorService.class.getSimpleName()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
