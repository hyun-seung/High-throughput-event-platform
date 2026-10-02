package event.api.events;

import event.common.events.EventSubmission;
import event.common.events.EventTopics;
import event.common.events.EventType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

class KafkaEventRequestPublisherTest {
    @Test
    void sendsOriginalExecutionToReceivedTopic() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, EventSubmission> kafka = mock(KafkaTemplate.class);
        var event = new EventSubmission("00000000-0000-0000-0000-000000000001", 42,
                "client-event-1", "01012345678", EventType.GENERAL, Map.of("message", "hello"), false,
                Instant.parse("2026-10-02T00:00:00Z"));
        var send = CompletableFuture.completedFuture((org.springframework.kafka.support.SendResult<String, EventSubmission>) null);
        when(kafka.send(EventTopics.RECEIVED, event.executionId(), event)).thenReturn(send);

        assertSame(send, new KafkaEventRequestPublisher(kafka).publish(event));
        verify(kafka).send(EventTopics.RECEIVED, event.executionId(), event);
    }
}
