package messaging.common.messages;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageOriginCodecTest {
    @Test
    void originRoundTripKeepsCustomerMessageIdAndClientMsgIdSeparate() {
        var original = new MessageSubmission("00000000-0000-0000-0000-000000000001", 42L,
                "customer-message-7", "01012345678", MessageCategory.ALERT,
                Map.of("message", "hello"), true, Instant.parse("2026-10-02T00:00:00Z"));
        JsonMapper mapper = JsonMapper.builder().build();
        String eventJson = mapper.writeValueAsString(original);
        assertTrue(eventJson.contains("\"clientMsgId\""));
        assertFalse(eventJson.contains("\"executionId\""));

        var item = MessageOriginCodec.encode(original, mapper);
        assertEquals("RECEIVED", item.get("status").s());
        assertEquals(original.messageId(), item.get(MessageOriginCodec.MESSAGE_ID).s());
        assertFalse(item.containsKey("client_event_id"));
        assertEquals("DELIVERY#" + original.clientMsgId(), item.get("pk").s());
        assertEquals(MessagePublicationIndex.bucket(original.clientMsgId()), item.get(MessagePublicationIndex.BUCKET).s());
        assertEquals(Long.toString(original.receivedAt().plusSeconds(60).toEpochMilli()),
                item.get(MessagePublicationIndex.DUE).n());
        assertEquals(original, MessageOriginCodec.decode(item, mapper));
    }
}
