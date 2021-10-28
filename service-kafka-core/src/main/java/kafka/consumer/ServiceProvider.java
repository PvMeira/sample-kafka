package kafka.consumer;

import java.util.concurrent.Callable;

public record ServiceProvider<T>(ServiceFactory<T> factory) implements Callable<Void> {

    public Void call() throws Exception {

        var factoryService = factory.create();
        try (var service = new KafkaService(factoryService.getConsumerGroup(),
                factoryService.getTopic(),
                factoryService::parse,
                factoryService.getCustomProperties())) {
            service.run();
        }
        return null;
    }
}
