package consumer;

import json.User;
import kafka.consumer.KafkaService;
import kafka.consumer.Service;
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
import java.util.concurrent.ExecutionException;

public class ReadingReportService implements Service<Message<User>> {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var serviceSelf = new ReadingReportService();
        try (var service = new KafkaService<>(ReadingReportService.class.getSimpleName(),
                "ECOMMERCE_USER_GENERATE_READING_REPORT",
                serviceSelf::parse,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()))) {
            service.run();
        }

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
}
