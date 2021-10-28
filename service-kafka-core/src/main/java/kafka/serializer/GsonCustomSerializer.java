package kafka.serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import kafka.dto.Message;
import kafka.dto.MessageAdapter;
import org.apache.kafka.common.serialization.Serializer;

public class GsonCustomSerializer<T> implements Serializer<T> {

    private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();


    @Override
    public byte[] serialize(String s, T t) {
        return gson.toJson(t).getBytes();
    }

}
