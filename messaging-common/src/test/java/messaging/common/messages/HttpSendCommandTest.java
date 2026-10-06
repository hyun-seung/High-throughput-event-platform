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
    void rejectsAProviderIdLongerThanFortyBytes() {
        assertThrows(IllegalArgumentException.class, () -> new HttpProviderRequest("a".repeat(41), 42,
                "GENERAL", "01012345678", Map.of("message", "hello"),
                Instant.parse("2026-10-02T00:00:00Z")));
    }

    @Test
    void keepsTheSameClientMsgIdAcrossCarrierChangesAndRetries() {
        var body = new HttpProviderRequest("request-1", 42, "GENERAL", "01012345678",
                Map.of("message", "hello"), Instant.parse("2026-10-02T00:00:00Z"));
        var command = new HttpSendCommand("attempt-1", HttpCarrier.SKT, 1,
                Instant.parse("2026-10-02T03:00:00Z"), body);
        var retry = new HttpSendCommand("attempt-2", HttpCarrier.KT, 2, command.deadlineAt(), body);

        assertEquals("attempt-1", command.attemptId());
        assertEquals(2, retry.invocation());
        assertEquals("request-1", command.request().clientMsgId());
        assertEquals(command.request().clientMsgId(), retry.request().clientMsgId());
        String providerJson = JsonMapper.builder().build().writeValueAsString(command.request());
        assertTrue(providerJson.contains("\"clientMsgId\":\"request-1\""));
        assertFalse(providerJson.contains("\"sendRequestId\""));
        assertFalse(providerJson.contains("\"invocation\""));
    }
}
