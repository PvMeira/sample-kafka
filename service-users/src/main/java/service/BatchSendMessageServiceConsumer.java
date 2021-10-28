package service;

import br.com.pvmeira.database.LocalDataBase;
import json.User;
import kafka.consumer.ServiceConsumer;
import kafka.consumer.ServiceRunner;
import kafka.desserializar.GsonDeserializer;
import kafka.dto.Message;
import kafka.producer.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageServiceConsumer implements ServiceConsumer<String> {

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();
    private final LocalDataBase dataBase;

    BatchSendMessageServiceConsumer() throws SQLException {
        this.dataBase = new LocalDataBase("users_database");
        this.dataBase.createIfNotExists("create table Users (uuid varchar(200) primary key, email varchar (200))");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(BatchSendMessageServiceConsumer::new).start(1);
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
                                                                        , record.value().getId().continueWithId(BatchSendMessageServiceConsumer.class.getSimpleName()));
        }
        System.out.println("------------------------------------------");
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS";
    }

    @Override
    public String getConsumerGroup() {
        return BatchSendMessageServiceConsumer.class.getSimpleName();
    }

    @Override
    public Map getCustomProperties() {
        return Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
    }

    private List<User> getAllUsers() throws SQLException {
        var select = this.dataBase.preparedStatement("select uuid from USERS").executeQuery();
        var resultList = new ArrayList<User>();
        while (select.next()) {
            resultList.add(new User(select.getString(1)));
        }
        return resultList;
    }


}
