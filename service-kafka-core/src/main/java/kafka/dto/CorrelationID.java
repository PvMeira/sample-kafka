package kafka.dto;

import java.util.UUID;

public class CorrelationID {

    private final String id;

    public CorrelationID(String name) {
        this.id = name + "(" + UUID.randomUUID() +")";
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "CorrelationID{" +
                "id='" + id + '\'' +
                '}';
    }

    public CorrelationID continueWithId(String currentId) {
    return new CorrelationID(id + "-"+ currentId)
;    }
}
