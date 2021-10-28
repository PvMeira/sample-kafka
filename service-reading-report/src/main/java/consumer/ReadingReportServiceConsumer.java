package consumer;

import json.User;
import kafka.consumer.ServiceRunner;
import kafka.consumer.ServiceConsumer;
import kafka.desserializar.GsonDeserializer;
import kafka.dto.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class ReadingReportServiceConsumer implements ServiceConsumer<User> {

    public static void main(String[] args) {
       new ServiceRunner<>(ReadingReportServiceConsumer::new).start(1);
    }

    @Override
    public void parse(ConsumerRecord<String, Message<User>> message) {
        var user = message.value().getPayload();
        System.out.println("------------------------------------------");
        System.out.println("Reading report order recive: " + user.getUuid());
        var target = new File(user.getPath());
        var file =  new File("service-reading-report/src/main/resources/report.txt").toPath();

        target.getParentFile().mkdirs();
        try {
            var created = Files.copy(file, target.toPath(), StandardCopyOption.REPLACE_EXISTING);
            Files.write(created, user.getUuid().getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("File Created on : " + target.getAbsolutePath());
        System.out.println("------------------------------------------");
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT";
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportServiceConsumer.class.getSimpleName();
    }

    @Override
    public Map getCustomProperties() {
        return Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
    }
}
