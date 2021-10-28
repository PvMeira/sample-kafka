package kafka.consumer;

import br.com.pvmeira.database.LocalDataBase;
import json.Order;
import kafka.desserializar.GsonDeserializer;
import kafka.dto.CorrelationID;
import kafka.dto.Message;
import kafka.producer.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import utils.FraudUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorServiceConsumer implements ServiceConsumer<Order> {

    private final LocalDataBase dataBase;
    private Order order;

    public static void main(String[] args) {
        new ServiceRunner(FraudDetectorServiceConsumer::new).start(1);
    }

    FraudDetectorServiceConsumer() throws SQLException {
        this.dataBase = new LocalDataBase("frauds_database");
        this.dataBase.createIfNotExists("create table Orders (uuid varchar(200) primary key, is_fraud boolean)");
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("------------------------------------------");
        System.out.println("Processing order");
        order = record.value().getPayload();
        System.out.println("Order ID for this request was : ".concat(order.getOrderId()));

        if (orderAlreadyProcessed(order.getOrderId())) {
            System.out.println("This order was already processed, skipping ....");
            System.out.println("------------------------------------------");

            return;
        } else {
            if (FraudUtils.isSuspectedOrder(record.value().getPayload())) {
                this.insert(order.getOrderId(), true);
                System.out.println("Order is a Fraud");
                this.sendMessage("ECOMMERCE_ORDER_REJECT", record.value().getPayload().getEmail(), record.value().getPayload(), record.value().getId());
            } else {
                this.insert(order.getOrderId(), false);
                System.out.println("Order processed with success");
                this.sendMessage("ECOMMERCE_ORDER_APPROVED", record.value().getPayload().getEmail(), record.value().getPayload(), record.value().getId());

            }
        }
        System.out.println("------------------------------------------");
    }

    private boolean orderAlreadyProcessed(String orderId) throws SQLException {
        var select = this.dataBase.preparedStatement("SELECT uuid FROM Orders where uuid = ? limit 1");
        select.setString(1, orderId);
        return select.executeQuery().next();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorServiceConsumer.class.getSimpleName();
    }

    @Override
    public Map getCustomProperties() {
        return Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
    }

    private void sendMessage(String topic, String key, Order order, CorrelationID id) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            orderDispatcher.send(topic, key, order, id.continueWithId(FraudDetectorServiceConsumer.class.getSimpleName()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insert(String uuid, Boolean isFraud) throws SQLException {
        var statement = this.dataBase.preparedStatement("insert into Orders (uuid, is_fraud) " +
                "values (?,?)");
        statement.setString(1, uuid);
        statement.setBoolean(2, isFraud);
        statement.execute();
        System.out.println("Orders " + uuid + " inserted in database.");
    }
}
