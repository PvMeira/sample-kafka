package utils;

import json.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;

public class FraudUtils {

    public static boolean isSuspectedOrder(ConsumerRecord<String, Order> record) {
        return record.value().getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
