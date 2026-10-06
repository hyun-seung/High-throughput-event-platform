package messaging.api.messages;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class MessageReceiveResponseTest {
    @Test
    void exposesTheStableClientMsgIdToTheCustomer() {
        var response = new MessageReceiveResponse("00000000-0000-0000-0000-000000000001", "RECEIVED");
        var json = JsonMapper.builder().build().writeValueAsString(response);

        assertEquals(response.clientMsgId(), JsonMapper.builder().build().readTree(json).get("clientMsgId").asText());
        assertFalse(json.contains("sendRequestId"));
        assertFalse(json.contains("executionId"));
    }
}
