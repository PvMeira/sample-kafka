package kafka.consumer;

import kafka.consumer.ServiceConsumer;

import java.sql.SQLException;

public interface ServiceFactory<T> {
    ServiceConsumer<T> create() throws SQLException;
}
