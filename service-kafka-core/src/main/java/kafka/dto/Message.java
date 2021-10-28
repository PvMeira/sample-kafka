package kafka.dto;

public class Message<T> {

    private final CorrelationID id;
    private final T payload;

    public Message(CorrelationID id, T payload) {
        this.id = id;
        this.payload = payload;
    }

    public CorrelationID getId() {
        return id;
    }

    public T getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id=" + id +
                ", payload=" + payload +
                '}';
    }
}
