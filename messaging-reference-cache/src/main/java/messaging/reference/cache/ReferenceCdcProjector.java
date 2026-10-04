package messaging.reference.cache;

import messaging.common.messages.MessageReferenceCacheKeys;
import messaging.common.messages.HttpCarrier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

/** Projects Debezium row events into the Redis keys read by PRE-SEND-MANAGER. */
@Component
public class ReferenceCdcProjector {
    private final StringRedisTemplate redis;
    private final JsonMapper mapper;

    public ReferenceCdcProjector(StringRedisTemplate redis, JsonMapper mapper) {
        this.redis = redis;
        this.mapper = mapper;
    }

    @SuppressWarnings("unchecked")
    public void apply(String topic, String keyJson, String valueJson) {
        // Debezium follows a delete envelope with a null tombstone. The delete already
        // removed the Redis entry; the tombstone is only for Kafka log compaction.
        if (valueJson == null) return;
        Map<String, Object> key = mapper.readValue(keyJson, Map.class);
        Map<String, Object> event = mapper.readValue(valueJson, Map.class);
        String operation = requiredString(event, "op");
        boolean deleted = "d".equals(operation);
        if (!deleted && !"c".equals(operation) && !"u".equals(operation) && !"r".equals(operation)) {
            throw new IllegalArgumentException("Unsupported reference CDC operation: " + operation);
        }
        switch (topic) {
            case ReferenceCdcTopics.CONTRACTS -> {
                long clientId = requiredLong(key, "client_id");
                String redisKey = MessageReferenceCacheKeys.contract(clientId);
                if (deleted) redis.delete(redisKey);
                else {
                    Map<String, Object> after = requiredRow(event);
                    if (requiredLong(after, "client_id") != clientId || !(after.get("enabled") instanceof Boolean)) {
                        throw new IllegalArgumentException("Contract CDC key and row disagree");
                    }
                    redis.opsForValue().set(redisKey, mapper.writeValueAsString(after));
                }
            }
            case ReferenceCdcTopics.PHONE_CARRIERS -> {
                String phone = requiredString(key, "phone_number");
                if (!phone.matches("010[0-9]{8}")) throw new IllegalArgumentException("Invalid phone CDC key");
                String redisKey = MessageReferenceCacheKeys.phoneCarrier(phone);
                if (deleted) redis.delete(redisKey);
                else {
                    Map<String, Object> after = requiredRow(event);
                    if (!phone.equals(requiredString(after, "phone_number"))) {
                        throw new IllegalArgumentException("Phone CDC key and row disagree");
                    }
                    String carrier = HttpCarrier.valueOf(requiredString(after, "carrier")).name();
                    redis.opsForValue().set(redisKey, carrier);
                }
            }
            default -> throw new IllegalArgumentException("Unexpected reference CDC topic: " + topic);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> requiredRow(Map<String, Object> event) {
        if (event.get("after") instanceof Map<?, ?> row) return (Map<String, Object>) row;
        throw new IllegalArgumentException("CDC event has no after row");
    }

    private static String requiredString(Map<String, Object> map, String field) {
        if (map != null && map.get(field) instanceof String value && !value.isBlank()) return value;
        throw new IllegalArgumentException("CDC field is missing: " + field);
    }

    private static long requiredLong(Map<String, Object> map, String field) {
        if (map != null && map.get(field) instanceof Number number) return number.longValue();
        throw new IllegalArgumentException("CDC field is missing: " + field);
    }
}
