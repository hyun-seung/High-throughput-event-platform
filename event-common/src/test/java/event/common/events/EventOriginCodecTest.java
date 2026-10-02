package event.common.events;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EventOriginCodecTest {
    @Test
    void originRoundTripKeepsCustomerAndExecutionIdentifiersSeparate() {
        var original = new EventSubmission("00000000-0000-0000-0000-000000000001", 42L,
                "customer-event-7", "01012345678", EventType.ALERT,
                Map.of("message", "hello"), true, Instant.parse("2026-10-02T00:00:00Z"));
        JsonMapper mapper = JsonMapper.builder().build();

        var item = EventOriginCodec.encode(original, mapper);
        assertEquals("RECEIVED", item.get("status").s());
        assertEquals("DELIVERY#" + original.executionId(), item.get("pk").s());
        assertEquals(original, EventOriginCodec.decode(item, mapper));
    }
}
