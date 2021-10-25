package service;

import json.Order;
import kafka.consumer.KafkaService;
import kafka.consumer.Service;
import kafka.desserializar.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService implements Service<Order> {

    private final Connection connection;
    public CreateUserService() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:sqlite:service-users/target/users_database.db");
        try{
            connection.createStatement().execute("create table Users (uuid varchar(200) primary key, email varchar (200))");
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        var createUserDetectorService = new CreateUserService();
        try (var service = new KafkaService<>(  CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserDetectorService::parse,
                Order.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()))) {
            service.run();
        }
    }

    @Override
    public void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("------------------------------------------");
        System.out.println("Processing order");
        System.out.println(record.key());
        System.out.println("Order ID for this request was : ".concat(record.value().getOrderId()));
        if (isNew(record.value().getEmail())){
            this.insertNew(record.value().getEmail());
        }
        System.out.println("------------------------------------------");
    }

    public boolean isNew(String email) throws SQLException {
        var statement = connection.prepareStatement("select uuid from Users " +
                "where email = ? limit 1");
        statement.setString(1, email);
        var results = statement.executeQuery();
        return !results.next();
    }

    public void insertNew(String email) throws SQLException {
        var statement = connection.prepareStatement("insert into Users (uuid, email) " +
                "values (?,?)");
        statement.setString(1, UUID.randomUUID().toString());
        statement.setString(2, email);
        statement.execute();
        System.out.println("User " + email + " inserted.");
    }

}
