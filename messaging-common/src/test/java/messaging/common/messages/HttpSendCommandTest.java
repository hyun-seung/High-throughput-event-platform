package messaging.common.messages;

import org.junit.jupiter.api.Test;

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
    void rejectsACommandWhoseWireBodyBelongsToAnotherExecution() {
        var body = new HttpProviderRequest("execution-1", 42, "GENERAL", "01012345678",
                Map.of("message", "hello"), Instant.parse("2026-10-02T00:00:00Z"), 1);

        assertThrows(IllegalArgumentException.class, () -> new HttpSendCommand("execution-2", "attempt-1", "send-request-1",
                HttpCarrier.SKT, Instant.parse("2026-10-02T03:00:00Z"), body));
    }

    @Test
    void requiresAStableSenderRequestIdDistinctFromTheCarrierAttempt() {
        var body = new HttpProviderRequest("execution-1", 42, "GENERAL", "01012345678",
                Map.of("message", "hello"), Instant.parse("2026-10-02T00:00:00Z"), 1);
        var command = new HttpSendCommand("execution-1", "attempt-1", "send-request-1",
                HttpCarrier.SKT, Instant.parse("2026-10-02T03:00:00Z"), body);

        assertEquals("attempt-1", command.attemptId());
        assertEquals("send-request-1", command.sendRequestId());
        assertThrows(IllegalArgumentException.class, () -> new HttpSendCommand("execution-1", "attempt-1", " ",
                HttpCarrier.SKT, command.deadlineAt(), body));
    }
}
