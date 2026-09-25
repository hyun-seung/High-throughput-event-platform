package event.delivery.result;

import event.common.delivery.DeliveryIds;
import event.common.lifecycle.DeliveryFinalized;
import org.springframework.stereotype.Component;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.Set;
import java.util.UUID;

/** Strict v1/v2 contract; Java type headers supplied by a producer are never used. */
@Component
public class FinalizedCodec {
    private final JsonMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();

    public DeliveryFinalized decode(String key, byte[] bytes) {
        try {
            if (bytes == null || bytes.length == 0 || bytes.length > 16_384) throw invalid();
            var event = mapper.readValue(bytes, DeliveryFinalized.class);
            validate(event);
            if (!event.deliveryId().equals(key)) throw invalid();
            return event;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (RuntimeException e) {
            // Do not include event contents in logs or exception messages.
            throw invalid();
        }
    }

    public void validate(DeliveryFinalized e) {
        if (e == null || (e.schemaVersion() != 1 && e.schemaVersion() != 2) || !"DeliveryFinalized".equals(e.eventType())
                || !uuid(e.requestKey()) || !uuid(e.deliveryId()) || !DeliveryFinalized.eventId(e.deliveryId()).equals(e.eventId())
                || e.tenantId() <= 0 || !text(e.deliveryType(), 100)
                || !Set.of("DELIVERED", "FAILED", "EXPIRED").contains(e.outcome() == null ? "" : e.outcome())
                || !text(e.reason(), 200) || (e.routeOrder() != 1 && e.routeOrder() != 2)
                || !text(e.provider(), 100) || !DeliveryIds.attemptId(e.deliveryId(), e.provider(), e.routeOrder(), 1).equals(e.attemptId())
                || e.occurredAt() == null || e.resultAt() == null || e.finalizedAt() == null || e.deadline() == null) {
            throw invalid();
        }
    }

    public String encode(DeliveryFinalized e) { return mapper.writeValueAsString(e); }

    public DeliveryFinalized stored(String json) { return mapper.readValue(json, DeliveryFinalized.class); }

    private static boolean text(String value, int max) { return value != null && !value.isBlank() && value.length() <= max; }
    private static boolean uuid(String value) {
        try { return value != null && UUID.fromString(value).toString().equals(value); }
        catch (IllegalArgumentException e) { return false; }
    }
    private static IllegalArgumentException invalid() { return new IllegalArgumentException("Invalid DeliveryFinalized v1/v2 contract"); }
}
