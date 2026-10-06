package messaging.api.messages;

import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageTopics;
import messaging.common.messages.MessageCategory;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

class KafkaMessageRequestPublisherTest {
    @Test
    void sendsOriginalExecutionToReceivedTopic() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, MessageSubmission> kafka = mock(KafkaTemplate.class);
        var event = new MessageSubmission("00000000-0000-0000-0000-000000000001", 42,
                "client-event-1", "01012345678", MessageCategory.GENERAL, Map.of("message", "hello"), null,
                Instant.parse("2026-10-02T00:00:00Z"));
        var send = CompletableFuture.completedFuture((org.springframework.kafka.support.SendResult<String, MessageSubmission>) null);
        when(kafka.send(MessageTopics.RECEIVED, event.clientMsgId(), event)).thenReturn(send);

        assertSame(send, new KafkaMessageRequestPublisher(kafka).publish(event));
        verify(kafka).send(MessageTopics.RECEIVED, event.clientMsgId(), event);
    }
}
