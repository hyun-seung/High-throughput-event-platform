package messaging.common.messages;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HttpSendCommandTest {
    @Test
    void eachCarrierHasAnIsolatedTopicWithTheSameCommandType() {
        assertEquals("message.skt.http.send.v1", MessageTopics.httpSend(HttpCarrier.SKT));
        assertEquals("message.kt.http.send.v1", MessageTopics.httpSend(HttpCarrier.KT));
        assertEquals("message.lgu.http.send.v1", MessageTopics.httpSend(HttpCarrier.LGU));
    }

    @Test
    void rejectsACommandWhoseWireBodyBelongsToAnotherMessage() {
        var body = new HttpProviderRequest("request-1:SKT:1", 42, "GENERAL", "01012345678",
                Map.of("message", "hello"), Instant.parse("2026-10-02T00:00:00Z"), 1);

        assertThrows(IllegalArgumentException.class, () -> new HttpSendCommand("request-2", "attempt-1",
                HttpCarrier.SKT, Instant.parse("2026-10-02T03:00:00Z"), body));
    }

    @Test
    void keepsOneMessageIdAndDerivesTheProviderIdForEachInvocation() {
        var body = new HttpProviderRequest("request-1:SKT:1", 42, "GENERAL", "01012345678",
                Map.of("message", "hello"), Instant.parse("2026-10-02T00:00:00Z"), 1);
        var command = new HttpSendCommand("request-1", "attempt-1",
                HttpCarrier.SKT, Instant.parse("2026-10-02T03:00:00Z"), body);

        assertEquals("attempt-1", command.attemptId());
        assertEquals("request-1", command.sendRequestId());
        assertEquals("request-1:SKT:1", command.request().clientMsgId());
        String providerJson = JsonMapper.builder().build().writeValueAsString(command.request());
        assertTrue(providerJson.contains("\"clientMsgId\":\"request-1:SKT:1\""));
        assertFalse(providerJson.contains("\"sendRequestId\""));
        assertThrows(IllegalArgumentException.class, () -> new HttpSendCommand("request-1", "attempt-1",
                HttpCarrier.KT, command.deadlineAt(), body));
    }
}
