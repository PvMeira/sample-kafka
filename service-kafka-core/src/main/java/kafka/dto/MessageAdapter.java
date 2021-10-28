package kafka.dto;

import com.google.gson.*;

import java.lang.reflect.Type;

public class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {
    @Override
    public JsonElement serialize(Message message, Type type, JsonSerializationContext jsonSerializationContext) {
        var jsonObj = new JsonObject();
        jsonObj.addProperty("type", message.getPayload().getClass().getName());
        jsonObj.add("payload", jsonSerializationContext.serialize(message.getPayload()));
        jsonObj.add("correlationId", jsonSerializationContext.serialize(message.getId()));
        return jsonObj;
    }

    @Override
    public Message deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        var obj = jsonElement.getAsJsonObject();
        var payloadType= obj.get("type").getAsString();
        var correlationId = (CorrelationID)jsonDeserializationContext.deserialize(obj.get("correlationId"), CorrelationID.class);
        try {
            var payload = jsonDeserializationContext.deserialize(obj.get("payload"), Class.forName(payloadType));
            return new Message(correlationId, payload);
        } catch (ClassNotFoundException e) {
            throw new JsonParseException("A error occur when the deserializer was trying to covert to the payload type",e);
        }

    }
}
