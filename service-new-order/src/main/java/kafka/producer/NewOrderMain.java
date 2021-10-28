package kafka.producer;

import json.Order;
import kafka.dto.CorrelationID;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
                for (int i = 0; i < 10; i++) {
                    String email =Math.random() + "%d@teste.com";
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER"
                            , email
                            , new Order(UUID.randomUUID().toString()
                                    , email
                                    , BigDecimal.valueOf(Math.random() * 5000 + 1))
                    , new CorrelationID(NewOrderMain.class.getSimpleName()));

            }
        }
    }

    }
