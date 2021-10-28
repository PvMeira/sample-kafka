package kafka.consumer;

import kafka.dto.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public interface ServiceConsumer<T> {

    void parse(ConsumerRecord<String, Message<T>> record) throws ExecutionException, InterruptedException, SQLException, IOException;

    String getTopic();

    String getConsumerGroup();

    Map getCustomProperties();
}
