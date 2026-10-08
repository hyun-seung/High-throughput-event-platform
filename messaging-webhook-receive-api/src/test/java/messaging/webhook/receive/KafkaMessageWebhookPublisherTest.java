package messaging.webhook.receive;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageTopics;
import messaging.common.messages.MessageWebhookBatch;
import messaging.common.messages.MessageWebhookResult;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

class KafkaMessageWebhookPublisherTest {
    @Test
    void entireWebhookBatchIsOneKafkaRecordKeyedByTraceId() {
        KafkaTemplate<String, MessageWebhookBatch> kafka = mock(KafkaTemplate.class);
        var batch = new MessageWebhookBatch("trace-1", "WEBHOOK", HttpCarrier.SKT,
                Instant.parse("2026-10-07T00:00:00Z"),
                List.of(new MessageWebhookResult("a".repeat(32), "success", null)));
        when(kafka.send(MessageTopics.MSG_RESULT, batch.traceId(), batch))
                .thenReturn(CompletableFuture.completedFuture(null));

        assertDoesNotThrow(() -> new KafkaMessageWebhookPublisher(kafka).publish(batch).join());

        verify(kafka, times(1)).send(MessageTopics.MSG_RESULT, batch.traceId(), batch);
    }
}
