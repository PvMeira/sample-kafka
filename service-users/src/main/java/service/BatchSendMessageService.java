package service;

import json.User;
import kafka.consumer.KafkaService;
import kafka.consumer.Service;
import kafka.desserializar.GsonDeserializer;
import kafka.dto.Message;
import kafka.producer.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService implements Service<Message<String>> {

    private final Connection connection;
    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();


    public BatchSendMessageService() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:sqlite:service-users/target/users_database.db");
        try{
            connection.createStatement().execute("create table Users (uuid varchar(200) primary key, email varchar (200))");
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        var batchSendMessageService = new BatchSendMessageService();
        try (var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchSendMessageService::parse,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()))) {
            service.run();
        }
    }

    @Override
    public void parse(ConsumerRecord<String, Message<String>> record) throws SQLException, ExecutionException, InterruptedException {
        System.out.println("------------------------------------------");
        System.out.println("Processing new Batch");
        System.out.println("Topic : " + record.value());

        var users = getAllUsers();
        System.out.println("Found a total of " + users.size() + " users ");
        for (User user: users) {
            var future = userDispatcher.sendAsync(  record.value().getPayload()
                                                                        , user.getUuid()
                                                                        , user
                                                                        , record.value().getId().continueWithId(BatchSendMessageService.class.getSimpleName()));
        }
        System.out.println("------------------------------------------");
    }

    private List<User> getAllUsers() throws SQLException {
        var select = connection.prepareStatement("select uuid from USERS").executeQuery();
        var resultList = new ArrayList<User>();
        while (select.next()) {
            resultList.add(new User(select.getString(1)));
        }
        return resultList;
    }


}
