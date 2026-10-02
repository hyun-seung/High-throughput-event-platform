package event.common.events;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HttpSendCommandTest {
    @Test
    void eachCarrierHasAnIsolatedTopicWithTheSameCommandType() {
        assertEquals("event.skt.http.send.v1", EventTopics.httpSend(HttpCarrier.SKT));
        assertEquals("event.kt.http.send.v1", EventTopics.httpSend(HttpCarrier.KT));
        assertEquals("event.lgu.http.send.v1", EventTopics.httpSend(HttpCarrier.LGU));
    }

    @Test
    void rejectsACommandWhoseWireBodyBelongsToAnotherExecution() {
        var body = new HttpProviderRequest("execution-1", 42, "GENERAL", "01012345678",
                Map.of("message", "hello"), Instant.parse("2026-10-02T00:00:00Z"), 1);

        assertThrows(IllegalArgumentException.class, () -> new HttpSendCommand("execution-2", "attempt-1",
                HttpCarrier.SKT, Instant.parse("2026-10-02T03:00:00Z"), body));
    }
}
