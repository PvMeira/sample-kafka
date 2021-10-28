package service;

import br.com.pvmeira.database.LocalDataBase;
import json.Order;
import kafka.consumer.ServiceConsumer;
import kafka.consumer.ServiceRunner;
import kafka.desserializar.GsonDeserializer;
import kafka.dto.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserServiceConsumer implements ServiceConsumer<Order> {

    private final LocalDataBase dataBase;

    CreateUserServiceConsumer() throws SQLException {
        this.dataBase = new LocalDataBase("users_database");
        this.dataBase.createIfNotExists("create table Users (uuid varchar(200) primary key, email varchar (200))");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(CreateUserServiceConsumer::new).start(1);
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("------------------------------------------");
        System.out.println("Processing order");
        System.out.println(record.key());
        System.out.println("Order ID for this request was : ".concat(record.value().getPayload().getOrderId()));
        if (isNew(record.value().getPayload().getEmail())){
            this.insertNew(record.value().getPayload().getEmail());
        }
        System.out.println("------------------------------------------");
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserServiceConsumer.class.getSimpleName();
    }

    @Override
    public Map getCustomProperties() {
        return Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
    }

    public boolean isNew(String email) throws SQLException {
        var statement = this.dataBase.preparedStatement("select uuid from Users " +
                "where email = ? limit 1");
        statement.setString(1, email);
        var results = statement.executeQuery();
        return !results.next();
    }

    public void insertNew(String email) throws SQLException {
        var statement = this.dataBase.preparedStatement("insert into Users (uuid, email) " +
                "values (?,?)");
        statement.setString(1, UUID.randomUUID().toString());
        statement.setString(2, email);
        statement.execute();
        System.out.println("User " + email + " inserted.");
    }

}
