package kafka.producer;

import json.Email;
import json.Order;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<Email>()) {
                for (int i = 0; i < 10; i++) {
                    String email =Math.random() + "%d@teste.com";
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER"
                            , email
                            , new Order(UUID.randomUUID().toString()
                                    , email
                                    , BigDecimal.valueOf(Math.random() * 5000 + 1)));
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, new Email("teste@teste.com", "this is the body"));
                }
            }
        }
    }

    }
