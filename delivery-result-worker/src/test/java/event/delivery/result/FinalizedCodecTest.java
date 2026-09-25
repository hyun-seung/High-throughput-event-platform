package event.delivery.result;

import event.common.delivery.DeliveryIds;
import event.common.lifecycle.DeliveryFinalized;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class FinalizedCodecTest {
    final FinalizedCodec codec = new FinalizedCodec();

    static DeliveryFinalized event(String outcome, int route) {
        String id = UUID.randomUUID().toString(), provider = route == 1 ? "mock-provider" : "tcp-provider";
        Instant now = Instant.parse("2026-09-25T01:02:03.123456789Z");
        return new DeliveryFinalized(1, "DeliveryFinalized", DeliveryFinalized.eventId(id), id, 42,
                "RCS", outcome, outcome, route, DeliveryIds.attemptId(id, provider, route, 1), provider,
                now.minusSeconds(30), now, now.plusSeconds(1), now.plusSeconds(60));
    }

    @Test void roundTripIncludesNanosecondsAndBothRoutes() {
        for (int route : new int[]{1, 2}) for (String outcome : new String[]{"DELIVERED", "FAILED", "EXPIRED"}) {
            var e = event(outcome, route);
            assertEquals(e, codec.decode(e.deliveryId(), codec.encode(e).getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test void rejectsUnknownSchemaFieldsTrailingJsonAndMalformedPayloads() {
        var e = event("DELIVERED", 1);
        String json = codec.encode(e);
        for (String invalid : new String[]{"null", "{}", "bad-json", json + " {}",
                json.replace("\"schemaVersion\":1", "\"schemaVersion\":2"),
                json.substring(0, json.length() - 1) + ",\"unexpected\":true}"}) {
            assertThrows(IllegalArgumentException.class, () -> codec.decode(e.deliveryId(), invalid.getBytes(StandardCharsets.UTF_8)));
        }
        assertThrows(IllegalArgumentException.class, () -> codec.decode(e.deliveryId(), null));
        assertThrows(IllegalArgumentException.class, () -> codec.decode(e.deliveryId(), new byte[16_385]));
    }

    @Test void rejectsWrongKeyIdsMissingDatesAndUnknownOutcome() {
        var e = event("DELIVERED", 1);
        String json = codec.encode(e);
        assertThrows(IllegalArgumentException.class, () -> codec.decode("wrong", json.getBytes(StandardCharsets.UTF_8)));
        for (String invalid : new String[]{json.replace(e.eventId(), UUID.randomUUID().toString()),
                json.replace(e.attemptId(), UUID.randomUUID().toString()),
                json.replace("\"tenantId\":42", "\"tenantId\":0"),
                json.replace("\"routeOrder\":1", "\"routeOrder\":3"),
                json.replace("\"outcome\":\"DELIVERED\"", "\"outcome\":\"ACCEPTED\""),
                json.replace("\"occurredAt\":", "\"ignoredDate\":"),
                json.replace("\"provider\":\"mock-provider\"", "\"provider\":null")}) {
            assertThrows(IllegalArgumentException.class, () -> codec.decode(e.deliveryId(), invalid.getBytes(StandardCharsets.UTF_8)));
        }
    }
}
