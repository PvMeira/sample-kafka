package kafka.consumer;

import java.util.concurrent.Executors;

public class ServiceRunner<T> {

    private final ServiceProvider<T> provider;

    public ServiceRunner(ServiceFactory<T> factory) {
        this.provider = new ServiceProvider<>(factory);
    }

    public void start(int count) {

        var pool = Executors.newFixedThreadPool(count);
        for (int i = 0; i < count; i++) {
            pool.submit(provider);
        }
    }
}
