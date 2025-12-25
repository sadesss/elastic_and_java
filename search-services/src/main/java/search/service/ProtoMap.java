package search.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.util.Map;

public final class ProtoMap {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private ProtoMap() {}

    public static Map<String, Object> toMap(Message msg) {
        try {
            // protobuf -> JSON string
            String json = JsonFormat.printer()
                    .includingDefaultValueFields() // можно убрать, если не хочешь пустые поля
                    .print(msg);

            // JSON -> Map
            return MAPPER.readValue(json, MAP_TYPE);
        } catch (Exception e) {
            throw new RuntimeException("proto->map convert failed: " + e.getMessage(), e);
        }
    }
}
