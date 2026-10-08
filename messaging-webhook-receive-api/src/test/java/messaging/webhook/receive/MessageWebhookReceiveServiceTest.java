package messaging.webhook.receive;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageWebhookBatch;
import messaging.common.messages.MessageWebhookResult;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MessageWebhookReceiveServiceTest {
    private final MessageWebhookPublisher publisher = mock(MessageWebhookPublisher.class);
    private final MessageWebhookReceiveService service = new MessageWebhookReceiveService(publisher,
            Clock.fixed(Instant.parse("2026-10-07T00:00:00Z"), ZoneOffset.UTC));
    private final List<MessageWebhookResult> results = List.of(
            new MessageWebhookResult("a".repeat(32), "success", null));

    @Test
    void oneWebhookRequestPublishesOneBatchAndAcknowledgesOnlyAfterKafka() {
        var kafka = new CompletableFuture<Void>();
        when(publisher.publish(any())).thenReturn(kafka);
        var accepted = service.accept(HttpCarrier.SKT, results);
        assertFalse(accepted.isDone());
        var batch = org.mockito.ArgumentCaptor.forClass(MessageWebhookBatch.class);
        verify(publisher).publish(batch.capture());
        assertEquals(HttpCarrier.SKT, batch.getValue().carrier());
        assertEquals("WEBHOOK", batch.getValue().source());
        assertEquals(results, batch.getValue().results());
        assertEquals(32, batch.getValue().traceId().length());
        kafka.complete(null);
        assertEquals(batch.getValue().traceId(), accepted.join().traceId());
    }

    @Test
    void invalidBatchNeverPublishesAndKafkaFailureIsNotAcknowledged() {
        assertThrows(IllegalArgumentException.class, () -> service.accept(HttpCarrier.SKT, List.of()));
        verifyNoInteractions(publisher);
        when(publisher.publish(any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));
        var failure = assertThrows(CompletionException.class,
                () -> service.accept(HttpCarrier.KT, results).join());
        assertInstanceOf(MessageWebhookReceiveService.PublicationUnconfirmed.class, failure.getCause());
    }

    @Test
    void credentialsAreDisabledByDefaultAndCannotBeSharedAcrossCarriers() {
        var disabled = new MessageWebhookProperties(null, null, null);
        assertEquals("", disabled.secret(HttpCarrier.SKT));
        String secret = "a".repeat(32);
        assertThrows(IllegalArgumentException.class,
                () -> new MessageWebhookProperties(secret, secret, "b".repeat(32)));
        assertThrows(IllegalArgumentException.class,
                () -> new MessageWebhookProperties("short", null, null));
    }
}
