package kafka.desserializar;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {

    public static final String TYPE_CONFIG = "kafka.desserializar.type_config";

    private Gson gson = new GsonBuilder().create();
    private Class<T> type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        try {
            this.type = (Class<T>)Class.forName(String.valueOf(configs.get(TYPE_CONFIG)));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Cannot cast type ".concat(e.getMessage()), e);
        }
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        return this.gson.fromJson(new String(bytes), type);
    }
}
