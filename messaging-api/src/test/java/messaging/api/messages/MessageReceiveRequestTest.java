package messaging.api.messages;

import messaging.common.messages.MessageCategory;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.*;

class MessageReceiveRequestTest {
    @Test
    void customerJsonUsesMessageIdAndCategory() {
        JsonMapper mapper = JsonMapper.builder().build();
        var request = mapper.readValue("""
                {"messageId":"customer-1","recipientNumber":"01012345678",
                 "messageCategory":"NOTI","payload":{"message":"hello"},
                 "secondarySendPayload":{"message":"fallback"}}
                """, MessageReceiveRequest.class);

        assertEquals("customer-1", request.messageId());
        assertEquals(MessageCategory.NOTI, request.messageCategory());
        assertEquals("fallback", request.secondarySendPayload().get("message"));
        String serialized = mapper.writeValueAsString(request);
        assertTrue(serialized.contains("\"messageId\""));
        assertTrue(serialized.contains("\"messageCategory\""));
        assertFalse(serialized.contains("eventId"));
        assertFalse(serialized.contains("eventType"));
    }

    @Test
    void secondarySendPayloadIsOptional() {
        var request = JsonMapper.builder().build().readValue("""
                {"messageId":"customer-1","recipientNumber":"01012345678",
                 "messageCategory":"NOTI","payload":{"message":"hello"}}
                """, MessageReceiveRequest.class);

        assertFalse(request.hasSecondarySendPayload());
    }
}
